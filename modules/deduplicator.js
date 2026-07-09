//urldedup
const { createClient } = require('@supabase/supabase-js');

const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_KEY
);

// Level 1 - Batched Supabase URL duplicate check
//
// Scoped by client_id + module_id (NOT submodule_id). An article already
// seen by ANY submodule under a given module is treated as "seen" for
// the whole module -- this avoids duplicate LLM calls and duplicate
// entries showing up on the same frontend tab when two submodules of the
// same module (e.g. Policy & Risk Monitor's "Tax & Financial Policy" and
// "Regulatory Compliance") both happen to fetch the same article.
// A different module (a different frontend tab) still gets its own
// independent check, since that's a genuinely separate view for the client.
const removeUrlDuplicates = async (articles, clientId, moduleId) => {
  if (!articles || articles.length === 0) return [];

  const incomingUrls = articles.map(a => a.url).filter(Boolean);

  try {
    const { data: existingRecords, error } = await supabase
      .from('processed_urls')
      .select('source_url')
      .eq('client_id', clientId)
      .eq('module_id', moduleId)
      .in('source_url', incomingUrls);

    if (error) throw error;

    const existingUrlSet = new Set(existingRecords.map(r => r.source_url));

    const cleanArticles = articles.filter(article => {
      if (existingUrlSet.has(article.url)) {
        console.log(`Duplicate URL skipped (module: ${moduleId}): ${article.url}`);
        return false;
      }
      return true;
    });
    // Store newly seen URLs so future runs (for this client+module) can eliminate them
if (cleanArticles.length > 0) {
  const rows = cleanArticles.map(article => ({
    client_id: clientId,
    module_id: moduleId,
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

    console.log(`URL check (module: ${moduleId}): ${cleanArticles.length} passed out of ${articles.length}`);
    return cleanArticles;

  } catch (error) {
    console.log('Supabase bulk check error:', error.message);
    return articles;
  }
};

module.exports = {
  removeUrlDuplicates
};