// test-icp-dimensions-v2.js
require('dotenv').config();
const { processArticlesForRelevance } = require('./modules/llmRelevanceProcessor');

const CLIENT_ID = 'b61b4d3b-caeb-457b-9971-636c83688ee4';
const MODULE_ID = '55c5ee19-bfca-468b-81b3-b89ca4f303c8';
const FAKE_SUBMODULE_ID = 'test-submodule-id';

const testCases = [
  {
    label: 'Named competitor (Lancôme) acquisition — expect CRITICAL',
    article: {
      title: 'Lancôme Acquires Korean Skincare Tech Startup for $85M',
      url: 'https://example.com/test-lancome-v2',
      text: `Lancôme, the prestige beauty division of L'Oréal, has completed the acquisition of
a South Korean skincare technology startup for approximately $85 million. The startup,
known for its AI-driven personalized skincare diagnostics platform, will be integrated
into Lancôme's anti-aging product development pipeline. Industry analysts say the deal
strengthens Lancôme's competitive position in the prestige skincare segment, particularly
in Asian markets where personalized beauty technology is gaining rapid traction. The
acquisition is expected to close in Q3 2026 and marks Lancôme's third major technology
acquisition in the past two years, signaling an aggressive push into tech-enabled
premium skincare.`,
      publishedDate: '2026-07-01',
    },
  },
  {
    label: 'Generic prestige skincare startup funding, no named competitor — expect HIGH',
    article: {
      title: 'Prestige Skincare Startup Glow Labs Raises $30M Series B',
      url: 'https://example.com/test-generic-prestige-v2',
      text: `Glow Labs, a prestige skincare startup specializing in clinically-backed
anti-aging formulations, announced today it has closed a $30 million Series B funding
round led by a consortium of consumer-focused venture capital firms. The company plans
to use the funding to expand its specialty retail distribution and accelerate product
development in its colour cosmetics line. Glow Labs currently sells through select
specialty retailers and its own e-commerce platform, and the new funding will support
entry into additional prestige retail channels over the next 18 months. The round
values the company at approximately $220 million, more than doubling its valuation
from its previous funding round two years ago.`,
      publishedDate: '2026-07-01',
    },
  },
  {
    label: 'Geography match (Singapore) prestige retail expansion, no named competitor — expect HIGH',
    article: {
      title: 'Prestige Beauty Retailer Radiance Group Opens 12 New Stores Across Singapore',
      url: 'https://example.com/test-singapore-v2',
      text: `Radiance Group, a prestige beauty specialty retailer, announced plans to open
12 new flagship stores across Singapore over the next year, as part of a broader
Southeast Asian expansion strategy focused on colour cosmetics and skincare. The
company has invested approximately $15 million in the expansion, targeting high-traffic
retail districts. Radiance Group's expansion comes amid growing demand for prestige
beauty products in Singapore, with the company citing a 22 percent increase in prestige
skincare sales in the region over the past year. The retailer plans to further expand
into Malaysia and India by 2027, following a similar specialty retail model.`,
      publishedDate: '2026-07-01',
    },
  },
];

async function runAllTests() {
  for (const test of testCases) {
    console.log(`\n=== ${test.label} ===`);
    await processArticlesForRelevance(
      [test.article],
      CLIENT_ID,
      'Cosmetics',
      'test_job_' + Date.now(),
      MODULE_ID,
      FAKE_SUBMODULE_ID
    );
  }
}

runAllTests();