require('dotenv').config();
const { createClient } = require('@supabase/supabase-js');
const supabase = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_KEY);

(async () => {
  const { data, error } = await supabase
    .from('processed_urls')
    .insert({
      client_id: 'b61b4d3b-caeb-457b-9971-636c83688ee4',
      module_id: '55c5ee19-bfca-468a-81b3-b89ca4f303c8',
      source_url: 'https://test-debug-url.example.com',
      title: 'debug test',
      published_date: null,
      created_at: new Date().toISOString()
    });

  console.log('data:', data);
  console.log('error:', error);
})();