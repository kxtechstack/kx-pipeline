const { createClient } = require('@supabase/supabase-js');

const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_KEY
);

// Level 1 - Batched Supabase URL duplicate check
const removeUrlDuplicates = async (articles, clientId) => {
  if (!articles || articles.length === 0) return [];

  const incomingUrls = articles.map(a => a.url).filter(Boolean);

  try {
    const { data: existingRecords, error } = await supabase
      .from('processed_urls')
      .select('source_url')
      .eq('client_id', clientId)
      .in('source_url', incomingUrls);

    if (error) throw error;

    const existingUrlSet = new Set(existingRecords.map(r => r.source_url));

    const cleanArticles = articles.filter(article => {
      if (existingUrlSet.has(article.url)) {
        console.log(`Duplicate URL skipped: ${article.url}`);
        return false;
      }
      return true;
    });
    // Store newly seen URLs so future runs can eliminate them
if (cleanArticles.length > 0) {
  const rows = cleanArticles.map(article => ({
    client_id: clientId,
    source_url: article.url,
    title: article.title,
    published_date: article.publishedDate || null,
    created_at: new Date().toISOString()
  }));

  const { error: insertError } = await supabase
    .from('processed_urls')
    .insert(rows);

  if (insertError) {
    console.log('processed urls insert error:', insertError.message);
  }
}

    console.log(`URL check: ${cleanArticles.length} passed out of ${articles.length}`);
    return cleanArticles;

  } catch (error) {
    console.log('Supabase bulk check error:', error.message);
    return articles;
  }
};

module.exports = {
  removeUrlDuplicates
};