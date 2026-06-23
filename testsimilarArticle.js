/**
 * testSimilarArticles.js
 * ========================
 * Given a signal ID from policy_signals table:
 * 1. Fetches the signal from Supabase to get its article_id
 * 2. Finds that article's chunks in Qdrant
 * 3. Uses those vectors to search for similar chunks from OTHER articles
 * 4. Returns the top similar article titles
 *
 * Usage:
 *   node testSimilarArticles.js <signalId>
 *
 * Example:
 *   node testSimilarArticles.js 251ad301-9610-4b89-b8fc-15347ee8f3aa
 */

require('dotenv').config();
const { createClient } = require('@supabase/supabase-js');
const { QdrantClient } = require('@qdrant/js-client-rest');

const supabase = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_KEY);
const qdrant = new QdrantClient({
  url: process.env.QDRANT_URL,
  apiKey: process.env.QDRANT_API_KEY,
  checkCompatibility: false,
});

const POLICY_COLLECTION = process.env.POLICY_QDRANT_COLLECTION || 'policy_articles';
const TOP_SIMILAR = 3; // how many similar articles to return

const run = async () => {
  const signalId = process.argv[2];
  if (!signalId) {
    console.log('Usage: node testSimilarArticles.js <signalId>');
    process.exit(1);
  }

  console.log(`\nLooking up signal: ${signalId}`);

  // Step 1 - Fetch signal from Supabase
  const { data: signal, error } = await supabase
    .from('policy_signals')
    .select('*')
    .eq('id', signalId)
    .single();

  if (error || !signal) {
    console.log('Signal not found:', error?.message);
    process.exit(1);
  }

  console.log(`Signal title: ${signal.signal_title}`);
  console.log(`Article ID: ${signal.article_id}`);
  console.log(`Industry: ${signal.industry}`);
  console.log(`Client ID: ${signal.client_id}`);

  if (!signal.article_id) {
    console.log('\n❌ No article_id found on this signal — cannot search Qdrant.');
    console.log('This signal was stored before article_id was added to the pipeline.');
    process.exit(1);
  }

  // Step 2 - Find this article's chunks in Qdrant
  console.log('\nFinding chunks in Qdrant for this article...');
  const chunksResult = await qdrant.scroll(POLICY_COLLECTION, {
    filter: {
      must: [
        { key: 'article_id', match: { value: signal.article_id } },
      ],
    },
    limit: 5,
    with_vectors: true,
    with_payload: true,
  });

  if (!chunksResult.points || chunksResult.points.length === 0) {
    console.log('❌ No chunks found in Qdrant for this article_id.');
    console.log('This article may have been stored before article_id was added to Qdrant payload.');
    process.exit(1);
  }

  console.log(`Found ${chunksResult.points.length} chunk(s) for this article.`);

  // Debug: see what shape the vector actually comes back as
  const rawVector = chunksResult.points[0].vector;
  console.log('Raw vector type:', typeof rawVector);
  console.log('Raw vector (first 50 chars):', JSON.stringify(rawVector)?.slice(0, 50));

  const searchVector = Array.isArray(rawVector)
    ? rawVector
    : rawVector?.default
    ? rawVector.default
    : rawVector && typeof rawVector === 'object' && Object.keys(rawVector).length > 0
    ? Object.values(rawVector)[0]
    : null;

  if (!searchVector || !Array.isArray(searchVector)) {
    console.log('❌ Could not extract usable vector. Will fetch vector by point ID instead...');

    // Fallback: use recommend instead of search, using the point ID directly
    const pointId = chunksResult.points[0].id;
    console.log(`Using point ID for recommend: ${pointId}`);

    const recommended = await qdrant.recommend(POLICY_COLLECTION, {
      positive: [pointId],
      limit: 20,
      with_payload: true,
      filter: {
        must: [
          { key: 'client_id', match: { value: signal.client_id } },
          { key: 'industry', match: { value: signal.industry } },
        ],
      },
    });

    const seen = new Set();
    const similar = [];
    for (const point of recommended) {
      const articleId = point.payload.article_id;
      const title = point.payload.title;
      if (articleId === signal.article_id) continue;
      if (seen.has(articleId || title)) continue;
      seen.add(articleId || title);
      similar.push({ title, url: point.payload.url, score: point.score?.toFixed(3) });
      if (similar.length >= TOP_SIMILAR) break;
    }

    console.log('\n' + '='.repeat(60));
    console.log(`SIMILAR POLICY & RISK MOVEMENTS (via recommend):`);
    console.log('='.repeat(60));
    if (similar.length === 0) {
      console.log('No similar articles found.');
    } else {
      similar.forEach((s, i) => console.log(`${i + 1}. [${s.score}] ${s.title}\n   ${s.url}`));
    }
    return;
  }

  console.log(`Using vector of length: ${searchVector.length}`);

  // Step 3 - Search Qdrant for similar chunks from OTHER articles
  console.log('\nSearching for similar articles...');
  const searchResult = await qdrant.search(POLICY_COLLECTION, {
    vector: searchVector,
    limit: 20, // fetch more than needed so we can filter out same article
    with_payload: true,
    filter: {
      must: [
        { key: 'client_id', match: { value: signal.client_id } },
        { key: 'industry', match: { value: signal.industry } },
      ],
    },
  });

  // Step 4 - Filter out chunks from the same article, deduplicate by title
  const seen = new Set();
  const similar = [];

  for (const point of searchResult) {
    const articleId = point.payload.article_id;
    const title = point.payload.title;

    // Skip if same article as the signal we started with
    if (articleId === signal.article_id) continue;

    // Skip if we already have this article in results
    if (seen.has(articleId || title)) continue;

    seen.add(articleId || title);
    similar.push({
      title: point.payload.title,
      url: point.payload.url,
      score: point.score.toFixed(3),
      published_date: point.payload.published_date,
    });

    if (similar.length >= TOP_SIMILAR) break;
  }

  // Step 5 - Print results
  console.log('\n' + '='.repeat(60));
  console.log(`SIMILAR POLICY & RISK MOVEMENTS for:`);
  console.log(`"${signal.signal_title}"`);
  console.log('='.repeat(60));

  if (similar.length === 0) {
    console.log('No similar articles found.');
  } else {
    similar.forEach((s, i) => {
      console.log(`\n${i + 1}. [score: ${s.score}] ${s.title}`);
      console.log(`   URL: ${s.url}`);
      console.log(`   Date: ${s.published_date}`);
    });
  }

  console.log('\n' + '='.repeat(60));
};

run().catch(console.error);