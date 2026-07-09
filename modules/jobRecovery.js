/**
 * jobRecovery.js
 * ================
 * Two responsibilities:
 *
 * 1. detectStaleJobs() - finds jobs stuck in 'running' status with no
 *    update in the last N minutes (likely abandoned due to a crash,
 *    manual kill, or server restart) and marks them 'failed' with a
 *    clear reason, so the status table doesn't show forever-running
 *    jobs. Also releases the pipeline lock for that client+submodule,
 *    so a crash doesn't block retries until the lock's own TTL expires.
 *
 * 2. resumePipelineJob() - given a stalled/failed job, resumes
 *    processing from wherever it actually left off (based on
 *    current_stage and the still-intact raw Redis queue), instead of
 *    re-calling Exa from scratch.
 */

const { createClient } = require('@supabase/supabase-js');
const { releaseLock } = require('./queueManager');

const supabase = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_KEY);

const STALE_THRESHOLD_MINUTES = Number(process.env.STALE_JOB_THRESHOLD_MINUTES) || 10;

// ── Detect and mark stale jobs ───────────────────────────────────────────────
/**
 * Finds jobs with status='running' whose updated_at is older than the
 * stale threshold, and marks them 'failed'. Also releases the Redis lock
 * for that client+submodule, since the crashed process never reached its
 * own finally{} block to do so. Safe to call repeatedly -- already-
 * failed/completed jobs are untouched.
 */
const detectStaleJobs = async () => {
  const cutoff = new Date(Date.now() - STALE_THRESHOLD_MINUTES * 60 * 1000).toISOString();

  const { data: staleJobs, error } = await supabase
    .from('pipeline_job_status')
    .select('job_id, client_id, submodule_id, current_stage, updated_at')
    .eq('status', 'running')
    .lt('updated_at', cutoff);

  if (error) {
    console.log('[JobRecovery] Error checking for stale jobs:', error.message);
    return { found: 0, marked: 0 };
  }

  if (!staleJobs || staleJobs.length === 0) {
    return { found: 0, marked: 0 };
  }

  console.log(`[JobRecovery] Found ${staleJobs.length} stale job(s), marking as failed...`);

  for (const job of staleJobs) {
    await supabase
      .from('pipeline_job_status')
      .update({
        status: 'failed',
        failed_at_stage: job.current_stage,
        error_message: `Job stalled at stage '${job.current_stage}' with no update for over ${STALE_THRESHOLD_MINUTES} minutes -- likely interrupted (server crash or manual stop). Can be resumed from this stage.`,
        updated_at: new Date().toISOString(),
      })
      .eq('job_id', job.job_id);

    // Release the lock so this client+submodule isn't stuck waiting for
    // the lock's own TTL (up to 1 hour) even though we've already marked
    // the job failed and it's safe to retry now.
    if (job.client_id && job.submodule_id) {
      await releaseLock(job.client_id, job.submodule_id);
      console.log(`  [lock released] client: ${job.client_id}, submodule: ${job.submodule_id}`);
    } else {
      console.log(`  [!] Could not release lock for job ${job.job_id} -- missing client_id or submodule_id`);
    }

    console.log(`  [marked failed] ${job.job_id} (was stuck at: ${job.current_stage})`);
  }

  return { found: staleJobs.length, marked: staleJobs.length };
};

// ── Periodic background check ────────────────────────────────────────────────
/**
 * Runs detectStaleJobs() once immediately (catches anything left over
 * from before this server start), then on a repeating interval.
 */
const startStaleJobWatcher = (intervalMinutes = 5) => {
  console.log(`[JobRecovery] Starting stale job watcher (checks every ${intervalMinutes}min, threshold ${STALE_THRESHOLD_MINUTES}min)`);

  detectStaleJobs(); // run once immediately on startup

  setInterval(() => {
    detectStaleJobs();
  }, intervalMinutes * 60 * 1000);
};

module.exports = {
  detectStaleJobs,
  startStaleJobWatcher,
};