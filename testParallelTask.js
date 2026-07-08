/**
 * testParallelTask.js
 * =====================
 * Standalone test script for Parallel's TASK API -- this is the
 * "agent" product Govind mentioned, and it's DIFFERENT from the
 * Search API we already tested (see testParallel.js).
 *
 * Search API  -> gives you a list of articles/URLs (like Exa/Tavily)
 * Task API    -> you give it a research question, it searches AND
 *                synthesizes a structured answer itself, with
 *                citations + confidence levels
 *
 * This script asks the Task API to find recent banking regulation
 * events AND structure them the same way your policy_signals table
 * expects (title, category, impact_level, country, summary) -- so we
 * can see if it could realistically replace BOTH your fetch step AND
 * your own LLM relevance/extraction step, not just the fetch step.
 *
 * NOT wired into the pipeline. Just for inspection.
 *
 * Usage:
 *   node testParallelTask.js
 */

require('dotenv').config();
const Parallel = require('parallel-web').default;

const parallel = new Parallel({
  apiKey: process.env.PARALLEL_API_KEY,
});

// Same real client prompt used in the Search API test, so results
// are comparable apples-to-apples.
const TEST_INPUT = 'Recent regulatory changes, policy announcements, compliance requirements, government rulings, legal and legislative developments, and enforcement actions directly affecting the Banking industry. Focus strictly on: new laws, central bank directives, AML and KYC requirements, capital adequacy rules, sanctions updates, licensing changes, payment system regulations, and official announcements from financial regulators, central banks, ministries of finance, and supervisory authorities. Exclude entirely: mergers, acquisitions, funding rounds, product launches, partnerships, and general business news. Return only distinct, dated events -- NOT roundup articles, outlook pieces, or predictions covering multiple topics.';

// Ask the Task API to return an array of signals shaped like your
// policy_signals table. Trimmed to 5 fields (the API's own warning
// said base processor recommends max 5 properties) -- source_url and
// source_published_date dropped for this first test; can add back
// and bump to a higher processor tier if base can't handle it well.
const OUTPUT_SCHEMA = {
  type: 'json',
  json_schema: {
    type: 'object',
    properties: {
      signals: {
        type: 'array',
        items: {
          type: 'object',
          properties: {
            signal_title: { type: 'string' },
            category: { type: 'string' },
            impact_level: { type: 'string', enum: ['Low', 'Medium', 'High'] },
            country: { type: 'string' },
            summary: { type: 'string' },
          },
          required: ['signal_title', 'category', 'impact_level', 'country', 'summary'],
        },
      },
    },
    required: ['signals'],
  },
};

const runTest = async () => {
  console.log('========== SENDING TASK TO PARALLEL ==========');
  console.log('Input:', TEST_INPUT);
  console.log('Requested processor: base (cheapest tier, good starting point)');
  console.log('================================================\n');

  try {
    console.log('[Task API] Creating task run (this is async, may take a while)...');

    const taskRun = await parallel.taskRun.create({
      input: TEST_INPUT,
      processor: 'base',
      task_spec: {
        output_schema: OUTPUT_SCHEMA,
      },
    });

    console.log('Task run created:', taskRun.run_id || taskRun.id);
    console.log('Status:', taskRun.status);

    const runId = taskRun.run_id || taskRun.id;

    // The correct method is .result(), NOT .retrieve()/.get() -- those
    // only return run status/metadata. .result() blocks until the task
    // completes (or api_timeout is hit) and returns the actual output.
    console.log('\n[Task API] Waiting for result via taskRun.result() (this blocks until done)...');

    const runResult = await parallel.taskRun.result(runId, { api_timeout: 300 });

    console.log('\n========== FINAL TASK RESULT ==========');
    console.log(JSON.stringify(runResult, null, 2));

    if (runResult.output) {
      console.log('\n========== PARSED OUTPUT ONLY ==========');
      console.log(JSON.stringify(runResult.output, null, 2));
    }

  } catch (err) {
    console.error('Task API test FAILED:');
    console.error(err.response?.data || err.message);
    if (err.stack) console.error(err.stack);
  }
};

runTest();