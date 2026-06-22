//fetcher.js
const Exa = require('exa-js').default;

const exa = new Exa(process.env.EXA_API_KEY);

const fetchFromExa = async (promptText) => {

  const response = await exa.searchAndContents(promptText, {
  numResults: 100,
  type: 'auto',
  category: 'news',
  startPublishedDate: new Date(Date.now() - 60 * 24 * 60 * 60 * 1000).toISOString()
});

  console.log(`Fetched ${response.results.length} articles from Exa`);

  console.log("\n========== FIRST 10 ARTICLES FROM EXA ==========\n");

  response.results.slice(0, 10).forEach((article, index) => {
    console.log(`${index + 1}. ${article.title}`);
    console.log(`Date: ${article.publishedDate}`);
    console.log(`URL: ${article.url}`);
    console.log("-----------------------------------");
  });

  return response.results;
};

module.exports = { fetchFromExa };