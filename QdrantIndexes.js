/**
 * createQdrantIndexes.js
 * ========================
 * Creates the required payload indexes on the policy_articles
 * Qdrant collection so filtering by article_id, client_id,
 * and industry works correctly.
 *
 * Run once:
 *   node createQdrantIndexes.js
 */

require('dotenv').config();
const { QdrantClient } = require('@qdrant/js-client-rest');

const qdrant = new QdrantClient({
  url: process.env.QDRANT_URL,
  apiKey: process.env.QDRANT_API_KEY,
  checkCompatibility: false,
});

const COLLECTION = process.env.POLICY_QDRANT_COLLECTION || 'policy_articles';

const run = async () => {
  console.log(`Creating indexes on collection: ${COLLECTION}\n`);

  const indexes = [
    { field: 'article_id', type: 'keyword' },
    { field: 'client_id', type: 'keyword' },
    { field: 'industry', type: 'keyword' },
    { field: 'url', type: 'keyword' },
  ];

  for (const { field, type } of indexes) {
    try {
      await qdrant.createPayloadIndex(COLLECTION, {
        field_name: field,
        field_schema: type,
        wait: true,
      });
      console.log(`✅ Index created: ${field} (${type})`);
    } catch (err) {
      if (err?.data?.status?.error?.includes('already exists')) {
        console.log(`⏭  Already exists: ${field}`);
      } else {
        console.log(`❌ Failed: ${field} — ${err?.data?.status?.error || err.message}`);
      }
    }
  }

  console.log('\nDone. Run testSimilarArticles.js again now.');
};

run().catch(console.error);