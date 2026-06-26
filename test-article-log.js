require('dotenv').config();
const { createClient } = require('@supabase/supabase-js');

const supabase = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_KEY);

const testLog = async () => {
  console.log('Testing article_processing_log insert...');

  const { data, error } = await supabase
    .from('article_processing_log')
    .insert({
      job_id: 'test_job_123',
      client_id: 'b61b4d3b-caeb-457b-9971-636c83688ee4',
      article_url: 'https://example.com/test-article',
      article_title: 'Test Article Title',
      status: 'completed',
      error_message: null,
      retry_count: 0,
      processed_at: new Date().toISOString(),
    })
    .select();

  if (error) {
    console.error('❌ Insert failed:', error.message);
  } else {
    console.log('✅ Insert successful:', data);
  }
};

testLog();