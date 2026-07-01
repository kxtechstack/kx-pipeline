/**
 * jobStatusTracker.js
 * =====================
 * Writes permanent, queryable pipeline job status into Supabase
 * (pipeline_job_status table). This is separate from the Redis
 * status:jobId key -- Redis is for fast polling DURING a run,
 * this table is the permanent record you can query/report from
 * afterward, including for failed/crashed jobs.
 */

const { createClient } = require('@supabase/supabase-js');

const supabase = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_KEY);
const TABLE_NAME = 'pipeline_job_status';

// Create the initial row when a job starts
// Create the initial row when a job starts
const startJobTracking = async (jobId, clientId, promptText, submoduleId) => {
  await supabase.from(TABLE_NAME).insert({
    job_id: jobId,
    client_id: clientId,
    submodule_id: submoduleId,
    prompt_text: promptText,
    status: 'running',
    current_stage: 'fetching',
  });
};

// Update progress as the job moves through stages
const updateJobStage = async (jobId, stage, counts = {}) => {
  const updatePayload = {
    current_stage: stage,
    updated_at: new Date().toISOString(),
  };

  // Map any provided counts onto their column names
  if (counts.fetched !== undefined) updatePayload.count_fetched = counts.fetched;
  if (counts.afterUrlCheck !== undefined) updatePayload.count_after_url_check = counts.afterUrlCheck;
  if (counts.afterTopicDedup !== undefined) updatePayload.count_after_topic_dedup = counts.afterTopicDedup;
  if (counts.afterQualityFilter !== undefined) updatePayload.count_after_quality_filter = counts.afterQualityFilter;
  if (counts.pushedToQueue !== undefined) updatePayload.count_pushed_to_queue = counts.pushedToQueue;
  if (counts.afterLlm !== undefined) updatePayload.count_after_llm = counts.afterLlm;
  if (counts.storedFinal !== undefined) updatePayload.count_stored_final = counts.storedFinal;
  if (counts.rawQueueKey !== undefined) updatePayload.raw_queue_key = counts.rawQueueKey;
  if (counts.processedQueueKey !== undefined) updatePayload.processed_queue_key = counts.processedQueueKey;

  await supabase.from(TABLE_NAME).update(updatePayload).eq('job_id', jobId);
};

// Mark job as having finished all CURRENTLY BUILT stages (fetch -> dedup ->
// quality filter -> pushed to processed queue). This does NOT mean the
// full pipeline is done -- LLM relevance classification and final storage
// haven't been built yet. Status stays 'ready_for_llm' until that exists.
const completeJobTracking = async (jobId) => {
  await supabase.from(TABLE_NAME).update({
    status: 'ready_for_llm',
    current_stage: 'pushed_to_processed',
    updated_at: new Date().toISOString(),
  }).eq('job_id', jobId);
};

// Call this once the LLM step + final storage are actually built and
// a job has gone all the way through them -- THIS is true completion.
const markFullyCompleted = async (jobId) => {
  await supabase.from(TABLE_NAME).update({
    status: 'completed',
    current_stage: 'completed',
    completed_at: new Date().toISOString(),
    updated_at: new Date().toISOString(),
  }).eq('job_id', jobId);
};

// Mark job as failed, recording exactly where and why
const failJobTracking = async (jobId, stage, errorMessage) => {
  await supabase.from(TABLE_NAME).update({
    status: 'failed',
    failed_at_stage: stage,
    error_message: errorMessage,
    updated_at: new Date().toISOString(),
  }).eq('job_id', jobId);
};

module.exports = {
  startJobTracking,
  updateJobStage,
  completeJobTracking,
  markFullyCompleted,
  failJobTracking,
};