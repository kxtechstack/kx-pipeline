// fetcher.js
const Exa = require('exa-js').default;
const exa = new Exa(process.env.EXA_API_KEY);

const NINETY_DAYS_AGO = () => new Date(Date.now() - 90 * 24 * 60 * 60 * 1000).toISOString();

// ---------- EXA ----------
const fetchFromExa = async (promptText) => {
  const response = await exa.searchAndContents(promptText, {
    numResults: 5,
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

// ---------- PERPLEXITY (not implemented yet) ----------
const fetchFromPerplexity = async () => {
  throw new Error('Perplexity source is not implemented yet. Select Exa or Tavily.');
};

// ---------- REGISTRY ----------
const fetchers = {
  Exa: fetchFromExa,
  Tavily: fetchFromTavily,
  Perplexity: fetchFromPerplexity
};

const fetchArticles = async (source, promptText) => {
  const fetcher = fetchers[source];
  if (!fetcher) {
    throw new Error(`Unknown source: "${source}". Expected one of: ${Object.keys(fetchers).join(', ')}`);
  }
  return fetcher(promptText);
};

module.exports = { fetchFromExa, fetchFromTavily, fetchFromPerplexity, fetchArticles };