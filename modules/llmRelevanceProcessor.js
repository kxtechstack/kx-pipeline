/**
 * llmRelevanceProcessor.js
 * ===========================
 * Pulls articles from the processed Redis queue in small batches,
 * classifies each one as relevant/irrelevant via LM Studio, and for
 * relevant articles also extracts structured signal fields (title,
 * category, impact level, country, summary, business impact) in the
 * SAME LLM call -- no second call needed. Chunks + embeds the content
 * and stores everything in Qdrant + Supabase, including the new
 * policy_signals table that the frontend reads from.
 *
 * Prompt is fetched from Supabase (relevance_check_prompts table),
 * never hardcoded here.
 */

const { createClient } = require('@supabase/supabase-js');
const { QdrantClient } = require('@qdrant/js-client-rest');
const { pipeline } = require('@xenova/transformers');
const { v4: uuidv4 } = require('uuid');
const axios = require('axios');
const { pullProcessedBatch, getProcessedQueueLength } = require('./processedQueue');

const supabase = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_KEY);
// ── Log each article's processing result to Supabase ────────────────────────
const logArticle = async (jobId, clientId, article, status, errorMessage = null) => {
  await supabase.from('article_processing_log').insert({
    job_id: jobId,
    client_id: clientId,
    article_url: article.url,
    article_title: article.title,
    status,
    error_message: errorMessage,
    processed_at: new Date().toISOString(),
  });
};
const qdrant = new QdrantClient({
  url: process.env.QDRANT_URL,
  apiKey: process.env.QDRANT_API_KEY,
  checkCompatibility: false,
});

const LM_STUDIO_URL = process.env.LM_STUDIO_URL || 'http://localhost:1234/v1/chat/completions';
console.log("LM_STUDIO_URL =", LM_STUDIO_URL);
const LM_STUDIO_MODEL = process.env.LM_STUDIO_MODEL || 'llama-3.2-3b-instruct';

const POLICY_COLLECTION = process.env.POLICY_QDRANT_COLLECTION || 'policy_articles';
const VECTOR_SIZE = Number(process.env.EMBEDDING_VECTOR_SIZE) || 384;
const DELAY_BETWEEN_CALLS_MS = Number(process.env.LLM_CALL_DELAY_MS) || 1000;
const LLM_BATCH_SIZE = Number(process.env.LLM_BATCH_SIZE) || 10;
const CHUNK_SIZE = Number(process.env.CHUNK_SIZE) || 300;
const CHUNK_OVERLAP = Number(process.env.CHUNK_OVERLAP) || 50;
const TEXT_TRUNCATE_LENGTH = Number(process.env.LLM_TEXT_TRUNCATE_LENGTH) || 5000;

const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));

// ── Embedding model (reused for chunking/storing relevant articles) ─────────
let embedderPromise = null;
const getEmbedder = () => {
  if (!embedderPromise) embedderPromise = pipeline('feature-extraction', 'Xenova/all-MiniLM-L6-v2');
  return embedderPromise;
};
const embedText = async (text) => {
  const embedder = await getEmbedder();
  const output = await embedder(text, { pooling: 'mean', normalize: true });
  return Array.from(output.data);
};

// ── Fetch the active relevance-check prompt template from Supabase ──────────
const getRelevancePromptTemplate = async () => {
  const { data, error } = await supabase
    .from('relevance_check_prompts')
    .select('prompt_template')
    .eq('id', 'policy_risk_relevance_v1')
    .eq('is_active', true)
    .single();

  if (error || !data) {
    throw new Error(`Could not load relevance prompt from Supabase: ${error?.message}`);
  }

  return data.prompt_template;
};

// ── Build the final prompt by filling in placeholders ───────────────────────
const fillPromptTemplate = (template, industry, title, text) => {
  const truncatedText = (text || '').slice(0, TEXT_TRUNCATE_LENGTH);
  return template
    .replace(/{industry}/g, industry)
    .replace(/{title}/g, title || '')
    .replace(/{text}/g, truncatedText);
};

// ── Call LM Studio for relevance classification + signal extraction ─────────
const classifyArticle = async (promptTemplate, industry, article) => {
  const prompt = fillPromptTemplate(promptTemplate, industry, article.title, article.text);


  try {
    const response = await axios.post(LM_STUDIO_URL, {
      model: LM_STUDIO_MODEL,
      messages: [
        { role: 'system', content: 'You are a strict, precise classification assistant. You only respond with valid JSON, nothing else.' },
        { role: 'user', content: prompt },
      ],
      temperature: 0.1,
      max_tokens: 800,
    }, {
      timeout: 180000,
      headers: {
        "Content-Type": "application/json",
        "ngrok-skip-browser-warning": "true"
      }
    });

    const rawContent = response.data.choices[0].message.content.trim();

    // Strip markdown code fences if the model wrapped the JSON in them
    let cleaned = rawContent.replace(/^```json\s*/i, '').replace(/^```\s*/i, '').replace(/```\s*$/i, '').trim();

    // Some models occasionally add stray text before/after the JSON object,
    // or repeat the object twice. Extract just the first {...} block to be
    // more forgiving of this, instead of failing on the whole response.
    const jsonMatch = cleaned.match(/\{[\s\S]*?\}/);
    if (jsonMatch) {
      cleaned = jsonMatch[0];
    }

    let parsed;
    try {
      parsed = JSON.parse(cleaned);
    } catch (parseErr) {
      // Common failure: unescaped quotes inside string values, e.g.
      // "reason": "discusses the "new" rule" breaks strict JSON.
      // Try a lenient repair: escape quotes that appear inside string
      // values before re-parsing.
      try {
        const repaired = cleaned.replace(
          /"(reason|country|summary|signal_title|category|impact_level|source_type)"\s*:\s*"((?:[^"\\]|\\.)*)"/g,
          (match, key, value) => {
            return `"${key}": "${value}"`;
          }
        );
        parsed = JSON.parse(repaired);
      } catch (repairErr) {
        console.log(`      Raw response was: ${rawContent}`);
        throw parseErr;
      }
    }

    return {
      is_relevant: Boolean(parsed.is_relevant),
      reason: parsed.reason || 'No reason provided',
      signal_title: parsed.signal_title || article.title,
      category: parsed.category || 'Other Regulatory Risk',
      impact_level: parsed.impact_level || 'Low',
      source_type: parsed.source_type || 'News Report',
      country: parsed.country || 'Unknown',
      summary: parsed.summary || '',
      business_impact: Array.isArray(parsed.business_impact) ? parsed.business_impact : [],
    };

  } catch (err) {
    console.log(`  [!] LLM classification failed for "${article.title}": ${err.message}`);
    // Fail safe: if classification fails, mark irrelevant rather than
    // silently letting unclassified junk into the relevant dataset
    return {
      is_relevant: false,
      technical_failure: true,
      reason: `Classification failed: ${err.message}`,
      signal_title: article.title,
      category: 'Other Regulatory Risk',
      impact_level: 'Low',
      source_type: 'News Report',
      country: 'Unknown',
      summary: '',
      business_impact: [],
    };
  }
};

// ── Chunk text (simple paragraph/sentence based split) ──────────────────────
const chunkText = (text, chunkSize = CHUNK_SIZE, overlap = CHUNK_OVERLAP) => {
  if (!text) return [];
  const words = text.split(/\s+/);
  const chunks = [];
  let start = 0;
  while (start < words.length) {
    const end = start + chunkSize;
    chunks.push(words.slice(start, end).join(' '));
    start += chunkSize - overlap;
  }
  return chunks;
};

// ── Ensure Qdrant collection exists for relevant policy articles ────────────
const setupPolicyCollection = async () => {
  const collections = await qdrant.getCollections();
  const exists = collections.collections.some(c => c.name === POLICY_COLLECTION);
  if (!exists) {
    await qdrant.createCollection(POLICY_COLLECTION, {
      vectors: { size: VECTOR_SIZE, distance: 'Cosine' },
    });
    console.log(`[LLMProcessor] Created Qdrant collection '${POLICY_COLLECTION}'`);
  }
};

// ── Store a relevant article: chunk + embed + save to Qdrant and Supabase ───
const storeRelevantArticle = async (article, classification, clientId, industry, jobId) => {
  const chunks = chunkText(article.text);
  const articleId = uuidv4();

  const points = [];
  for (let i = 0; i < chunks.length; i++) {
    const vector = await embedText(chunks[i]);
    points.push({
      id: uuidv4(),
      vector,
      payload: {
        article_id: articleId,
        client_id: clientId,
        industry,
        title: article.title,
        url: article.url,
        chunk_index: i,
        chunk_text: chunks[i],
        published_date: article.publishedDate,
      },
    });
  }

  if (points.length > 0) {
    await qdrant.upsert(POLICY_COLLECTION, { points });
  }

  const { error: metaError } = await supabase.from('policy_articles_metadata').insert({
    article_id: articleId,
    client_id: clientId,
    industry,
    title: article.title,
    article_url: article.url,
    published_date: article.publishedDate,
    location: classification.country,
    is_relevant: true,
    relevance_reason: classification.reason,
  });
  if (metaError) console.error('[Storage] metadata insert error:', metaError.message);

  const { error: fullError } = await supabase.from('policy_articles_full').insert({
    article_id: articleId,
    client_id: clientId,
    industry,
    title: article.title,
    url: article.url,
    published_date: article.publishedDate,
    author: article.author,
    full_text: article.text,
    is_relevant: true,
    relevance_reason: classification.reason,
    location: classification.country,
    chunk_count: chunks.length,
    qdrant_collection_name: POLICY_COLLECTION,
    job_id: jobId,
  });
  if (fullError) console.error('[Storage] full insert error:', fullError.message);

  const { error: signalError } = await supabase.from('policy_signals').insert({
    article_id: articleId,
    client_id: clientId,
    industry,
    source_article_url: article.url,
    source_published_date: article.publishedDate,
    signal_title: classification.signal_title,
    category: classification.category,
    impact_level: classification.impact_level,
    source_type: classification.source_type,
    country: classification.country,
    summary: classification.summary,
    business_impact: classification.business_impact,
    job_id: jobId,
  });
  if (signalError) console.error('[Storage] signal insert error:', signalError.message);

  return chunks.length;
};

// ── Main processing function ──────────────────────────────────────────────
/**
 * @param {Array} articles - articles pulled from the processed Redis queue
 * @param {String} clientId
 * @param {String} industry
 * @param {String} jobId
 */
const processArticlesForRelevance = async (articles, clientId, industry, jobId) => {
  if (!articles || articles.length === 0) return { relevant: 0, irrelevant: 0 };

  await setupPolicyCollection();
  const promptTemplate = await getRelevancePromptTemplate();

  let relevantCount = 0;
  let irrelevantCount = 0;

  for (const article of articles) {
    console.log(`[LLMProcessor] Classifying: "${article.title}"`);

    const classification = await classifyArticle(promptTemplate, industry, article);

    if (classification.technical_failure) {
      await logArticle(jobId, clientId, article, 'failed', classification.reason);
      irrelevantCount++;
      console.log(`  [!] FAILED | ${classification.reason}`);
    } else if (classification.is_relevant) {
      const chunkCount = await storeRelevantArticle(
        article,
        classification,
        clientId,
        industry,
        jobId
      );
      await logArticle(jobId, clientId, article, 'completed');
      relevantCount++;
      console.log(
        `  [✓] RELEVANT (${chunkCount} chunks) | ${classification.category} | ${classification.impact_level} | ${classification.reason}`
      );
    } else {
      await logArticle(jobId, clientId, article, 'skipped', classification.reason);
      irrelevantCount++;
      console.log(`  [✗] IRRELEVANT | ${classification.reason}`);
    }

    await sleep(DELAY_BETWEEN_CALLS_MS);
  }

  console.log(`[LLMProcessor] Done. Relevant: ${relevantCount}, Irrelevant: ${irrelevantCount}`);
  return { relevant: relevantCount, irrelevant: irrelevantCount };
};

/**
 * Pulls articles from the processed Redis queue in batches of 10 (destructive
 * pop, since once classified these are done and should leave the queue),
 * and runs each batch through processArticlesForRelevance.
 *
 * @param {String} queueKey - e.g. 'processed:job_12345'
 * @param {String} clientId
 * @param {String} industry
 * @param {String} jobId
 * @param {Number} batchSize - default 10
 */
async function processQueueInBatches(queueKey, clientId, industry, jobId, batchSize = LLM_BATCH_SIZE) {
  let totalRelevant = 0;
  let totalIrrelevant = 0;
  let remaining = await getProcessedQueueLength(queueKey);

  console.log(`[LLMProcessor] Starting batch processing of ${queueKey} (${remaining} articles, batches of ${batchSize})`);

  let batchNumber = 1;
  while (remaining > 0) {
    console.log(`\n[LLMProcessor] --- Batch ${batchNumber} (${remaining} remaining) ---`);

    const batch = await pullProcessedBatch(queueKey, batchSize);
    const result = await processArticlesForRelevance(batch, clientId, industry, jobId);

    totalRelevant += result.relevant;
    totalIrrelevant += result.irrelevant;

    remaining = await getProcessedQueueLength(queueKey);
    batchNumber++;
  }

  console.log(`\n[LLMProcessor] ALL BATCHES DONE. Total relevant: ${totalRelevant}, Total irrelevant: ${totalIrrelevant}`);
  return { relevant: totalRelevant, irrelevant: totalIrrelevant };
}

module.exports = {
  processArticlesForRelevance,
  processQueueInBatches,
};