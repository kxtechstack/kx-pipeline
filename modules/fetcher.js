// fetcher.js
const Exa = require('exa-js').default;
const exa = new Exa(process.env.EXA_API_KEY);

const NINETY_DAYS_AGO = () => new Date(Date.now() - 90 * 24 * 60 * 60 * 1000).toISOString();

// ---------- EXA ----------
const fetchFromExa = async (promptText) => {
  const response = await exa.searchAndContents(promptText, {
    numResults:10,
    type: 'auto',
    category: 'news',
    startPublishedDate: NINETY_DAYS_AGO()
  });

  console.log(`Fetched ${response.results.length} articles from Exa`);

  return response.results.map(article => ({
    title: article.title,
    url: article.url,
    publishedDate: article.publishedDate,
    text: article.text || ''
  }));
};

// ---------- TAVILY ----------
const fetchFromTavily = async (promptText) => {
  const res = await fetch('https://api.tavily.com/search', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      api_key: process.env.TAVILY_API_KEY,
      query: promptText,
      topic: 'news',
      max_results: 5,
      days: 90,
      include_answer: false,
      include_raw_content: false
    })
  });

  if (!res.ok) {
    throw new Error(`Tavily API error: ${res.status} ${await res.text()}`);
  }

  const data = await res.json();
  console.log(`Fetched ${data.results.length} articles from Tavily`);

  return data.results.map(article => ({
    title: article.title,
    url: article.url,
    publishedDate: article.published_date || null,
    text: article.content || ''
  }));
};

// ---------- PARALLEL ----------
// Uses Parallel's BETA search endpoint (/v1beta/search), NOT the SDK's
// client.search() -- the stable SDK method silently ignores max_results,
// excerpts, and source_policy (confirmed via testParallel.js testing).
// The beta endpoint requires a special header to unlock those params.
//
// Note: source_policy.start_date is a soft freshness preference, not a
// hard guarantee -- some older/undated results can still come through.
// Also, unlike Exa's category:'news', Parallel has no way to exclude
// multi-topic "roundup" articles at the source -- that filtering still
// relies on qualityFilter.js and llmRelevanceProcessor.js downstream.
const fetchFromParallel = async (promptText) => {
  const ninetyDaysAgoDateOnly = new Date(Date.now() - 90 * 24 * 60 * 60 * 1000)
    .toISOString()
    .split('T')[0];

  const res = await fetch('https://api.parallel.ai/v1beta/search', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'x-api-key': process.env.PARALLEL_API_KEY,
      'parallel-beta': 'search-extract-2025-10-10'
    },
    body: JSON.stringify({
      objective: promptText,
      search_queries: [promptText],
      max_results: 5,
      excerpts: { max_chars_per_result: 5000 },
      source_policy: { start_date: ninetyDaysAgoDateOnly }
    })
  });

  if (!res.ok) {
    throw new Error(`Parallel API error: ${res.status} ${await res.text()}`);
  }

  const data = await res.json();
  const results = data.results || [];
  console.log(`Fetched ${results.length} articles from Parallel`);

  return results.map(article => ({
    title: article.title || '',
    url: article.url,
    publishedDate: article.publish_date || null,
    text: Array.isArray(article.excerpts) ? article.excerpts.join('\n\n') : ''
  }));
};

// ---------- PERPLEXITY (not implemented yet) ----------
const fetchFromPerplexity = async () => {
  throw new Error('Perplexity source is not implemented yet. Select Exa, Tavily, or Parallel.');
};

// ---------- REGISTRY ----------
const fetchers = {
  Exa: fetchFromExa,
  Tavily: fetchFromTavily,
  Parallel: fetchFromParallel,
  Perplexity: fetchFromPerplexity
};

const fetchArticles = async (source, promptText) => {
  const fetcher = fetchers[source];
  if (!fetcher) {
    throw new Error(`Unknown source: "${source}". Expected one of: ${Object.keys(fetchers).join(', ')}`);
  }
  return fetcher(promptText);
};

module.exports = { fetchFromExa, fetchFromTavily, fetchFromParallel, fetchFromPerplexity, fetchArticles };