/**
 * topicDedup.js
 * ==============
 * Embedding-based "same topic" duplicate detection using Qdrant.
 *
 * Scoped by client_id + module_id (NOT submodule_id) -- same reasoning
 * as deduplicator.js. An article judged semantically similar to one
 * already seen under ANY submodule of a given module is treated as a
 * duplicate for that whole module. A different module gets its own
 * independent check.
 *
 * How it works:
 *   1. Embed the article's title + snippet using a local model (no API calls, no cost)
 *   2. Search Qdrant's "dedup_titles" collection for similar vectors,
 *      filtered by client_id + module_id and a 60-day recency window
 *   3. If a close match is found -> it's the same story, drop it
 *   4. If not -> keep it, and immediately store its embedding in Qdrant
 *      so later articles (this batch or future runs) can be compared to it
 *
 * This collection ("dedup_titles") is SEPARATE from your RAG collection.
 * It only stores title-level vectors for dedup purposes, not full article
 * content. Nothing here is meant for the RAG/Q&A system.
 */

const { QdrantClient } = require('@qdrant/js-client-rest');
const { pipeline } = require('@xenova/transformers');
const { v4: uuidv4 } = require('uuid');

const QDRANT_URL = process.env.QDRANT_URL;
const QDRANT_API_KEY = process.env.QDRANT_API_KEY;

const DEDUP_COLLECTION = 'dedup_titles';
const VECTOR_SIZE = 384; // all-MiniLM-L6-v2 output size
const SIMILARITY_THRESHOLD = 0.78; // cosine similarity >= this -> treat as duplicate
const RECENCY_WINDOW_DAYS = 60;

const qdrant = new QdrantClient({
  url: QDRANT_URL,
  apiKey: QDRANT_API_KEY,
});

// ── Lazy-load the embedding model once, reuse across calls ──────────────────
let embedderPromise = null;
const getEmbedder = () => {
  if (!embedderPromise) {
    console.log('[TopicDedup] Loading embedding model (first call only)...');
    embedderPromise = pipeline('feature-extraction', 'Xenova/all-MiniLM-L6-v2');
  }
  return embedderPromise;
};

const embedText = async (text) => {
  const embedder = await getEmbedder();
  const output = await embedder(text, { pooling: 'mean', normalize: true });
  return Array.from(output.data); // Float32Array -> plain array
};

// Build the text used for dedup embedding: title + a short snippet of body content.
const SNIPPET_LENGTH = 400;

const buildEmbeddingText = (article) => {
  const title = article.title || '';
  let snippet = '';

  if (article.text) {
    const cleaned = article.text
      .replace(/[\u200B-\u200F\u202A-\u202E\uFEFF]/g, '')
      .replace(/\s+/g, ' ')
      .trim();
    snippet = cleaned.slice(0, SNIPPET_LENGTH);
  }

  return snippet ? `${title}. ${snippet}` : title;
};

// ── Setup: make sure the Qdrant collection exists ───────────────────────────
const setupDedupCollection = async () => {
  const collections = await qdrant.getCollections();
  const exists = collections.collections.some(c => c.name === DEDUP_COLLECTION);

  if (exists) {
    console.log(`[TopicDedup] Collection '${DEDUP_COLLECTION}' already exists.`);
    return;
  }

  await qdrant.createCollection(DEDUP_COLLECTION, {
    vectors: { size: VECTOR_SIZE, distance: 'Cosine' },
  });

  // Index client_id, module_id, and published_date_ts for fast filtered search
  await qdrant.createPayloadIndex(DEDUP_COLLECTION, {
    field_name: 'client_id',
    field_schema: 'keyword',
  });
  await qdrant.createPayloadIndex(DEDUP_COLLECTION, {
    field_name: 'module_id',
    field_schema: 'keyword',
  });
  await qdrant.createPayloadIndex(DEDUP_COLLECTION, {
    field_name: 'published_date_ts',
    field_schema: 'integer',
  });

  console.log(`[TopicDedup] Collection '${DEDUP_COLLECTION}' created with indexes.`);
};

// ── Main dedup function ──────────────────────────────────────────────────────
/**
 * @param {Array} articles - articles that already passed the URL dedup check
 * @param {String} clientId
 * @param {String} moduleId
 * @returns {Array} unique articles (same-topic duplicates removed)
 */
const removeSameTopicArticles = async (articles, clientId, moduleId) => {
  if (!articles || articles.length === 0) return [];

  await setupDedupCollection();

  const cutoffTs = Date.now() - RECENCY_WINDOW_DAYS * 24 * 60 * 60 * 1000;
  const uniqueArticles = [];

  for (const article of articles) {
    if (!article.title) {
      uniqueArticles.push(article);
      continue;
    }

    const textForEmbedding = buildEmbeddingText(article);
    const vector = await embedText(textForEmbedding);

    // Search Qdrant for similar titles, scoped to this client + module + recency window.
    // NOTE: using search() instead of query() here -- testing confirmed that
    // query()'s query_filter parameter is silently ignored by the installed
    // version of @qdrant/js-client-rest, causing cross-client data leakage.
    // search() with `filter` correctly respects the filter conditions.
    const searchResultRaw = await qdrant.search(DEDUP_COLLECTION, {
      vector,
      limit: 1,
      filter: {
        must: [
          { key: 'client_id', match: { value: clientId } },
          { key: 'module_id', match: { value: moduleId } },
          { key: 'published_date_ts', range: { gte: cutoffTs } },
        ],
      },
      with_payload: true,
    });

    const topMatch = searchResultRaw[0];

    if (topMatch && topMatch.score >= SIMILARITY_THRESHOLD) {
      console.log(
        `[DUPLICATE] module=${moduleId} score=${topMatch.score.toFixed(3)} | "${article.title}" ~ "${topMatch.payload.title}"`
      );
      continue; // drop — same topic already seen in this module
    }

    if (topMatch) {
      console.log(
        `[no match] module=${moduleId} best score=${topMatch.score.toFixed(3)} (below ${SIMILARITY_THRESHOLD}) | "${article.title}" vs "${topMatch.payload.title}"`
      );
    }

    // Not a duplicate -> keep it, and store its embedding for future comparisons
    uniqueArticles.push(article);

    const publishedTs = article.publishedDate
      ? new Date(article.publishedDate).getTime()
      : Date.now();

    await qdrant.upsert(DEDUP_COLLECTION, {
      points: [
        {
          id: uuidv4(),
          vector,
          payload: {
            client_id: clientId,
            module_id: moduleId,
            title: article.title,
            url: article.url,
            published_date_ts: publishedTs,
          },
        },
      ],
    });
  }

  console.log(`[TopicDedup] module=${moduleId}: ${uniqueArticles.length} unique out of ${articles.length}`);
  return uniqueArticles;
};

module.exports = {
  removeSameTopicArticles,
};