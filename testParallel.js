/**
 * testParallel.js
 * =================
 * Standalone test script for Parallel.ai's Search API.
 * NOT wired into the KX Pipeline -- just used to inspect the raw
 * response shape and compare result quality against Exa/Tavily
 * before writing fetchFromParallel() in fetcher.js.
 *
 * Usage:
 *   1. npm install parallel-web
 *   2. Set PARALLEL_API_KEY in your .env (or export it in shell)
 *   3. node testParallel.js
 */

require('dotenv').config();
const Parallel = require('parallel-web').default;

const parallel = new Parallel({
  apiKey: process.env.PARALLEL_API_KEY,
});

// Use the SAME kind of prompt you'd normally send to Exa/Tavily,
// so the comparison is apples-to-apples. This is a real client prompt
// (Banking industry, policy/risk monitor).
const TEST_OBJECTIVE = 'Recent regulatory changes, policy announcements, compliance requirements, government rulings, legal and legislative developments, and enforcement actions directly affecting the Banking industry. Focus strictly on: new laws, central bank directives, AML and KYC requirements, capital adequacy rules, sanctions updates, licensing changes, payment system regulations, and official announcements from financial regulators, central banks, ministries of finance, and supervisory authorities. Exclude entirely: mergers, acquisitions, funding rounds, product launches, partnerships, and general business news.';

// Short keyword queries pulled from the objective's core themes --
// Parallel's Search API wants 2-3 short keyword queries ALONGSIDE the
// natural-language objective (not a replacement for it).
const TEST_KEYWORDS = ['banking regulation 2026', 'central bank AML KYC directives'];

const runTest = async () => {
  console.log('========== SDK INTROSPECTION ==========');
  console.log('Top-level keys on client:', Object.keys(parallel));
  if (parallel.beta) {
    console.log('Keys under client.beta:', Object.keys(parallel.beta));
  } else {
    console.log('client.beta does not exist on this installed version.');
  }
  console.log('========================================\n');

  console.log('========== SENDING REQUEST TO PARALLEL ==========');
  console.log('Objective:', TEST_OBJECTIVE);
  console.log('Search queries:', TEST_KEYWORDS);
  console.log('===================================================\n');

  const callSearch = async () => {
    // The SDK's client.search() hits the STABLE endpoint (/v1/search),
    // which only accepts objective + search_queries -- that's why every
    // extra field kept getting rejected above.
    //
    // The fuller Search API (max_results, excerpts, source_policy) lives
    // at a separate BETA endpoint (/v1beta/search) and requires a special
    // header: "parallel-beta: search-extract-2025-10-10"
    //
    // Testing that directly with raw fetch, bypassing the SDK, to see if
    // this unlocks the advanced params.
    const ninetyDaysAgo = new Date(Date.now() - 90 * 24 * 60 * 60 * 1000)
      .toISOString()
      .split('T')[0];

    const betaBody = {
      objective: TEST_OBJECTIVE,
      search_queries: TEST_KEYWORDS,
      max_results: 5,
      excerpts: { max_chars_per_result: 5000 },
      source_policy: { start_date: ninetyDaysAgo },
    };

    console.log('Sending to /v1beta/search with beta header:', betaBody);

    const betaResponse = await fetch('https://api.parallel.ai/v1beta/search', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'x-api-key': process.env.PARALLEL_API_KEY,
        'parallel-beta': 'search-extract-2025-10-10',
      },
      body: JSON.stringify(betaBody),
    });

    if (!betaResponse.ok) {
      const errorText = await betaResponse.text();
      throw new Error(`Beta endpoint failed: ${betaResponse.status} ${errorText}`);
    }

    return betaResponse.json();
  };

  try {
    const result = await callSearch();

    console.log('========== RAW RESPONSE (full JSON) ==========');
    console.log(JSON.stringify(result, null, 2));
    console.log('================================================\n');

    console.log(`Got ${result.results?.length || 0} results.\n`);

    // Log each result's field names so we can see EXACTLY what's available
    // (title? url? published date? some other key entirely?)
    (result.results || []).forEach((item, i) => {
      console.log(`--- Result ${i + 1} ---`);
      console.log('Available fields:', Object.keys(item));
      console.log('Title:', item.title);
      console.log('URL:', item.url);
      console.log('Publish date:', item.publish_date);
      console.log('Excerpts (full array):', item.excerpts);
      console.log('');
    });

  } catch (err) {
    console.error('Parallel API test FAILED:');
    console.error(err.response?.data || err.message);
  }
};

runTest();