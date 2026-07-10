// delete-client-module-data.js
require('dotenv').config();
const { createClient } = require('@supabase/supabase-js');
const { QdrantClient } = require('@qdrant/js-client-rest');
const supabase = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_KEY);
const qdrant = new QdrantClient({ url: process.env.QDRANT_URL, apiKey: process.env.QDRANT_API_KEY, checkCompatibility: false });
const CLIENT_ID = 'b61b4d3b-caeb-457b-9971-636c83688ee4';
const MODULE_ID = '55c5ee19-bfca-468b-81b3-b89ca4f303c8';
const POLICY_COLLECTION = process.env.POLICY_QDRANT_COLLECTION || 'policy_articles';

async function deleteAll() {
  // 1. Delete from Qdrant - policy_articles
  const qdrantResult = await qdrant.delete(POLICY_COLLECTION, {
    filter: {
      must: [
        { key: 'client_id', match: { value: CLIENT_ID } },
        { key: 'module_id', match: { value: MODULE_ID } },
      ],
    },
    wait: true,
  });
  console.log('Qdrant policy_articles delete result:', qdrantResult.status);

  // 1b. Delete from Qdrant - dedup_titles
  const dedupResult = await qdrant.delete('dedup_titles', {
    filter: {
      must: [
        { key: 'client_id', match: { value: CLIENT_ID } },
        { key: 'module_id', match: { value: MODULE_ID } },
      ],
    },
    wait: true,
  });
  console.log('Qdrant dedup_titles delete result:', dedupResult.status);

  // 2. Delete from Supabase tables
  const tables = [
    'policy_articles_metadata',
    'policy_articles_full',
    'policy_signals',
    'processed_urls',
    'daily_highlights',
  ];
  for (const table of tables) {
    const { error, count } = await supabase
      .from(table)
      .delete({ count: 'exact' })
      .eq('client_id', CLIENT_ID)
      .eq('module_id', MODULE_ID);
    if (error) {
      console.log(`${table}: ERROR - ${error.message}`);
    } else {
      console.log(`${table}: deleted ${count} rows`);
    }
  }
  console.log('Done.');
}
deleteAll();