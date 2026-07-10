require('dotenv').config();
const { QdrantClient } = require('@qdrant/js-client-rest');

const client = new QdrantClient({ url: process.env.QDRANT_URL, apiKey: process.env.QDRANT_API_KEY });

async function checkCoverage() {
  const collectionName = 'policy_articles';

  let offset = undefined;
  let total = 0;
  let withModuleId = 0;
  let withoutModuleId = 0;
  const missingExamples = [];

  do {
    const result = await client.scroll(collectionName, {
      limit: 100,
      offset,
      with_payload: true,
      with_vector: false,
    });

    for (const point of result.points) {
      total++;
      if (point.payload && point.payload.module_id) {
        withModuleId++;
      } else {
        withoutModuleId++;
        if (missingExamples.length < 5) {
          missingExamples.push({ id: point.id, payload: point.payload });
        }
      }
    }

    offset = result.next_page_offset;
  } while (offset);

  console.log('Total points:', total);
  console.log('With module_id:', withModuleId);
  console.log('WITHOUT module_id:', withoutModuleId);
  console.log('Sample missing points:', JSON.stringify(missingExamples, null, 2));
}

checkCoverage();