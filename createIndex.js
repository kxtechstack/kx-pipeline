require('dotenv').config();
const { QdrantClient } = require('@qdrant/js-client-rest');

const qdrant = new QdrantClient({
  url: process.env.QDRANT_URL,
  apiKey: process.env.QDRANT_API_KEY,
  checkCompatibility: false,
});

const POLICY_COLLECTION = process.env.POLICY_QDRANT_COLLECTION || 'policy_articles';

const createIndexes = async () => {
  try {
    await qdrant.createPayloadIndex(POLICY_COLLECTION, {
      field_name: 'article_id',
      field_schema: 'keyword',
    });
    console.log('✅ article_id index created');

    await qdrant.createPayloadIndex(POLICY_COLLECTION, {
      field_name: 'client_id',
      field_schema: 'keyword',
    });
    console.log('✅ client_id index created');

    await qdrant.createPayloadIndex(POLICY_COLLECTION, {
      field_name: 'industry',
      field_schema: 'keyword',
    });
    console.log('✅ industry index created');

  } catch (err) {
    console.error('Error:', err.message);
  }
};

createIndexes();