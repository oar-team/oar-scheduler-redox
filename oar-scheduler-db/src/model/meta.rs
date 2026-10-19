//! Database requests needed by the standalone (Rust) meta-scheduler.
//!
//! These operations are the Rust counterpart of the `oar/lib/job_handling.py`,
//! `oar/lib/node.py` and `oar/kao/karma.py` helpers used by the Python
//! meta-scheduler.

use crate::model::events::{EventLogHostnames, EventLogs};
use crate::model::gantt::{GanttJobsPredictions, GanttJobsResources};
use crate::model::job_types::AllJobTypes;
use crate::model::jobs::{FragJobs, JobStateLogs, Jobs};
use crate::model::moldable::MoldableJobDescriptions;
use crate::{Session, SessionDeleteStatement, SessionInsertStatement, SessionSelectStatement, SessionUpdateStatement};
use sea_query::{Alias, Expr, ExprTrait, Func, Iden, Query};
use sqlx::{Error, Row};
use std::collections::HashMap;

#[derive(Iden)]
enum Accounting {
    #[iden = "accounting"]
    Table,
    #[iden = "window_start"]
    WindowStart,
    #[iden = "window_stop"]
    WindowStop,
    #[iden = "accounting_project"]
    Project,
    #[iden = "accounting_user"]
    User,
    #[iden = "queue_name"]
    QueueName,
    #[iden = "consumption_type"]
    ConsumptionType,
    #[iden = "consumption"]
    Consumption,
}

/// A single event log entry, as returned by [`get_job_events`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JobEvent {
    pub ev_type: String,
    pub date: i64,
}

/// Inserts a job in the `frag_jobs` table so that it is later killed by the
/// frag job handling process. Mirrors `oar.lib.job_handling.frag_job`.
///
/// Returns `true` if the job was newly flagged, `false` if it was already
/// flagged.
pub fn frag_job(session: &Session, job_id: i64) -> Result<bool, Error> {
    let now = session.get_now();
    let inserted = session.runtime.block_on(async {
        let existing = Query::select()
            .column(FragJobs::FragIdJob)
            .from(FragJobs::Table)
            .and_where(Expr::col(FragJobs::FragIdJob).eq(job_id))
            .fetch_all(session)
            .await?;
        if !existing.is_empty() {
            return Ok(false);
        }
        Query::insert()
            .into_table(FragJobs::Table)
            .columns(vec![FragJobs::FragIdJob, FragJobs::FragDate])
            .values_panic(vec![Expr::val(job_id), Expr::val(now)])
            .execute(session)
            .await?;
        Ok::<bool, Error>(true)
    })?;
    if inserted {
        // `add_new_event` itself runs a blocking runtime: it must be called
        // outside of the `block_on` above.
        crate::model::events::add_new_event(
            session,
            "FRAG_JOB_REQUEST",
            job_id,
            format!("Meta-scheduler requested to frag the job {}", job_id).as_str(),
        );
    }
    Ok(inserted)
}

/// Returns the ids of the jobs currently flagged for deletion.
pub fn get_fragged_jobs(session: &Session) -> Result<Vec<i64>, Error> {
    session.runtime.block_on(async {
        let rows = Query::select()
            .column(FragJobs::FragIdJob)
            .from(FragJobs::Table)
            .fetch_all(session)
            .await?;
        Ok(rows.iter().map(|row| row.try_get(FragJobs::FragIdJob.unquoted()).unwrap_or(0)).collect())
    })
}

/// Returns all the events logged for a job, ordered by date ascending.
pub fn get_job_events(session: &Session, job_id: i64) -> Result<Vec<JobEvent>, Error> {
    session.runtime.block_on(async {
        let rows = Query::select()
            .columns(vec![EventLogs::Type, EventLogs::Date])
            .from(EventLogs::Table)
            .and_where(Expr::col(EventLogs::JobId).eq(job_id))
            .order_by(EventLogs::Date, sea_query::Order::Asc)
            .fetch_all(session)
            .await?;
        Ok(rows
            .iter()
            .map(|row| JobEvent {
                ev_type: row.try_get(EventLogs::Type.unquoted()).unwrap_or_default(),
                date: row.try_get(EventLogs::Date.unquoted()).unwrap_or(0),
            })
            .collect())
    })
}

/// Copies the current gantt scheduling decisions into the visualization
/// tables, after flushing them. Mirrors `update_gantt_visualization`.
pub fn update_gantt_visualization(session: &Session) -> Result<(), Error> {
    session.runtime.block_on(async {
        Query::delete().from_table(GanttVisuPredictions::Table).execute(session).await?;
        Query::delete().from_table(GanttVisuResources::Table).execute(session).await?;
        sqlx::query(
            "INSERT INTO gantt_jobs_predictions_visu (moldable_job_id, start_time) \
             SELECT moldable_job_id, start_time FROM gantt_jobs_predictions",
        )
        .execute(&session.pool)
        .await?;
        sqlx::query(
            "INSERT INTO gantt_jobs_resources_visu (moldable_job_id, resource_id) \
             SELECT moldable_job_id, resource_id FROM gantt_jobs_resources",
        )
        .execute(&session.pool)
        .await?;
        Ok(())
    })
}

#[derive(Iden)]
enum GanttVisuPredictions {
    #[iden = "gantt_jobs_predictions_visu"]
    Table,
}

#[derive(Iden)]
enum GanttVisuResources {
    #[iden = "gantt_jobs_resources_visu"]
    Table,
}

/// Details of a waiting interactive job whose start prediction must be sent to
/// the `oarsub` client. Mirrors `get_gantt_waiting_interactive_prediction_date`.
#[derive(Debug, Clone)]
pub struct InteractivePrediction {
    pub job_id: i64,
    pub info_type: String,
    pub start_time: i64,
    pub message: String,
}

/// Returns the waiting interactive jobs with an assigned start time in the
/// gantt, so that the meta-scheduler can notify the `oarsub` client.
pub fn get_gantt_waiting_interactive_prediction_date(session: &Session) -> Result<Vec<InteractivePrediction>, Error> {
    session.runtime.block_on(async {
        let rows = Query::select()
            .columns(vec![
                (Jobs::Table, Jobs::Id),
                (Jobs::Table, Jobs::InfoType),
                (Jobs::Table, Jobs::Message),
            ])
            .columns(vec![(GanttJobsPredictions::Table, GanttJobsPredictions::StartTime)])
            .from(Jobs::Table)
            .inner_join(
                MoldableJobDescriptions::Table,
                Expr::col((MoldableJobDescriptions::Table, MoldableJobDescriptions::JobId)).equals((Jobs::Table, Jobs::Id)),
            )
            .inner_join(
                GanttJobsPredictions::Table,
                Expr::col((GanttJobsPredictions::Table, GanttJobsPredictions::MoldableId))
                    .equals((MoldableJobDescriptions::Table, MoldableJobDescriptions::Id)),
            )
            .and_where(Expr::col((Jobs::Table, Jobs::State)).eq("Waiting"))
            .and_where(Expr::col((Jobs::Table, Jobs::Type)).eq("INTERACTIVE"))
            .and_where(Expr::col((Jobs::Table, Jobs::Reservation)).eq("None"))
            .fetch_all(session)
            .await?;
        Ok(rows
            .iter()
            .filter_map(|row| {
                Some(InteractivePrediction {
                    job_id: row.try_get(Jobs::Id.unquoted()).ok()?,
                    info_type: row.try_get(Jobs::InfoType.unquoted()).ok()?,
                    start_time: row.try_get(GanttJobsPredictions::StartTime.unquoted()).ok()?,
                    message: row.try_get(Jobs::Message.unquoted()).unwrap_or_default(),
                })
            })
            .collect())
    })
}

/// Which column the accounting sums are grouped by.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AccountingGroup {
    Project,
    User,
}

impl AccountingGroup {
    fn column(&self) -> Accounting {
        match self {
            AccountingGroup::Project => Accounting::Project,
            AccountingGroup::User => Accounting::User,
        }
    }
    fn column_name(&self) -> &'static str {
        match self {
            AccountingGroup::Project => "accounting_project",
            AccountingGroup::User => "accounting_user",
        }
    }
}

fn accounting_rows(
    session: &Session,
    queues: &[String],
    window_start: i64,
    window_stop: i64,
    group: Option<AccountingGroup>,
    window_stop_inclusive: bool,
) -> Result<Vec<(String, String, f64)>, Error> {
    session.runtime.block_on(async {
        let mut query = Query::select();
        match group {
            Some(group) => {
                query.columns(vec![group.column(), Accounting::ConsumptionType]);
            }
            None => {
                query.columns(vec![Accounting::ConsumptionType]);
            }
        }
        query
            .expr_as(Func::sum(Expr::col(Accounting::Consumption)), Alias::new("total_consumption"))
            .from(Accounting::Table)
            .and_where(Expr::col(Accounting::QueueName).is_in(queues.iter().cloned()))
            .and_where(Expr::col(Accounting::WindowStart).gte(window_start));
        if window_stop_inclusive {
            query.and_where(Expr::col(Accounting::WindowStop).lte(window_stop));
        } else {
            query.and_where(Expr::col(Accounting::WindowStop).lt(window_stop));
        }
        query.group_by_col(Accounting::ConsumptionType);
        if let Some(group) = group {
            query.group_by_col(group.column());
        }

        let rows = query.fetch_all(session).await?;
        Ok(rows
            .iter()
            .map(|row| {
                let key = match group {
                    Some(group) => row.try_get::<String, &str>(group.column_name()).unwrap_or_default(),
                    None => String::new(),
                };
                (
                    key,
                    row.try_get::<String, &str>(Accounting::ConsumptionType.unquoted()).unwrap_or_default(),
                    row.try_get::<f64, &str>("total_consumption").unwrap_or(0.0),
                )
            })
            .collect())
    })
}

fn split_consumption_maps(rows: Vec<(String, String, f64)>) -> (HashMap<String, f64>, HashMap<String, f64>) {
    let mut asked = HashMap::new();
    let mut used = HashMap::new();
    for (key, consumption_type, total) in rows {
        match consumption_type.as_str() {
            "ASKED" => {
                asked.insert(key, total);
            }
            "USED" => {
                used.insert(key, total);
            }
            _ => {}
        }
    }
    (asked, used)
}

/// Returns the summed accounting consumption for the given queues in
/// `[window_start, window_stop)`, as `(ASKED, USED)`.
/// Mirrors `oar.kao.karma.get_sum_accounting_window`.
pub fn get_sum_accounting_window(session: &Session, queues: &[String], window_start: i64, window_stop: i64) -> Result<(f64, f64), Error> {
    let rows = accounting_rows(session, queues, window_start, window_stop, None, false)?;
    let mut asked = 1.0;
    let mut used = 1.0;
    for (_key, consumption_type, total) in rows {
        match consumption_type.as_str() {
            "ASKED" => asked = total,
            "USED" => used = total,
            _ => {}
        }
    }
    Ok((asked, used))
}

/// Returns the summed accounting consumption per project for the given queues,
/// as `(ASKED, USED)` maps. Mirrors `get_sum_accounting_by_project`.
pub fn get_sum_accounting_by_project(
    session: &Session,
    queues: &[String],
    window_start: i64,
    window_stop: i64,
) -> Result<(HashMap<String, f64>, HashMap<String, f64>), Error> {
    let rows = accounting_rows(session, queues, window_start, window_stop, Some(AccountingGroup::Project), false)?;
    Ok(split_consumption_maps(rows))
}

/// Returns the summed accounting consumption per user for the given queues,
/// as `(ASKED, USED)` maps. Mirrors `get_sum_accounting_by_user`.
pub fn get_sum_accounting_by_user(
    session: &Session,
    queues: &[String],
    window_start: i64,
    window_stop: i64,
) -> Result<(HashMap<String, f64>, HashMap<String, f64>), Error> {
    let rows = accounting_rows(session, queues, window_start, window_stop, Some(AccountingGroup::User), true)?;
    Ok(split_consumption_maps(rows))
}

// ---------------------------------------------------------------------------
// Walltime change requests
// ---------------------------------------------------------------------------

#[derive(Iden)]
enum WalltimeChange {
    #[iden = "walltime_change"]
    Table,
    #[iden = "job_id"]
    JobId,
    #[iden = "pending"]
    Pending,
    #[iden = "force"]
    Force,
    #[iden = "delay_next_jobs"]
    DelayNextJobs,
    #[iden = "granted"]
    Granted,
    #[iden = "granted_with_force"]
    GrantedWithForce,
    #[iden = "granted_with_delay_next_jobs"]
    GrantedWithDelayNextJobs,
}

#[derive(Iden)]
enum AssignedResources {
    #[iden = "assigned_resources"]
    Table,
    #[iden = "moldable_job_id"]
    MoldableId,
    #[iden = "resource_id"]
    ResourceId,
    #[iden = "assigned_resource_index"]
    Index,
}

/// A running job with a pending walltime change request.
/// Mirrors `oar.lib.walltime.JobWalltimeChange`.
#[derive(Debug, Clone)]
pub struct WalltimeChangeJob {
    pub job_id: i64,
    pub queue_name: String,
    pub start_time: i64,
    pub user: String,
    pub name: String,
    /// The walltime of the currently assigned moldable (`moldable_walltime`).
    pub walltime: i64,
    pub pending: i64,
    pub force: String,
    pub delay_next_jobs: String,
    pub granted: i64,
    pub granted_with_force: i64,
    pub granted_with_delay_next_jobs: i64,
    pub rids: Vec<i32>,
    /// The job types, as `type name -> optional value` (e.g. `timesharing -> "user,*"`).
    pub types: HashMap<String, Option<String>>,
}

impl WalltimeChangeJob {
    pub fn type_value(&self, name: &str) -> Option<&str> {
        self.types.get(name).map(|v| v.as_deref().unwrap_or(""))
    }

    pub fn has_type(&self, name: &str) -> bool {
        self.types.contains_key(name)
    }

    /// The walltime currently granted to the job (`moldable_walltime` + granted).
    pub fn current_walltime(&self) -> i64 {
        self.walltime + self.granted
    }
}

/// Returns all running jobs with a non-zero pending walltime change request.
/// Mirrors `oar.lib.job_handling.get_jobs_with_walltime_change`.
pub fn get_jobs_with_walltime_change(session: &Session) -> Result<Vec<WalltimeChangeJob>, Error> {
    session.runtime.block_on(async {
        let rows = Query::select()
            .columns(vec![
                (Jobs::Table, Jobs::Id),
                (Jobs::Table, Jobs::QueueName),
                (Jobs::Table, Jobs::StartTime),
                (Jobs::Table, Jobs::User),
                (Jobs::Table, Jobs::Name),
            ])
            .column((MoldableJobDescriptions::Table, MoldableJobDescriptions::Walltime))
            .column((WalltimeChange::Table, WalltimeChange::Pending))
            .column((WalltimeChange::Table, WalltimeChange::Force))
            .column((WalltimeChange::Table, WalltimeChange::DelayNextJobs))
            .column((WalltimeChange::Table, WalltimeChange::Granted))
            .column((WalltimeChange::Table, WalltimeChange::GrantedWithForce))
            .column((WalltimeChange::Table, WalltimeChange::GrantedWithDelayNextJobs))
            .column((AssignedResources::Table, AssignedResources::ResourceId))
            .from(Jobs::Table)
            .inner_join(
                WalltimeChange::Table,
                Expr::col((WalltimeChange::Table, WalltimeChange::JobId)).equals((Jobs::Table, Jobs::Id)),
            )
            .inner_join(
                MoldableJobDescriptions::Table,
                Expr::col((Jobs::Table, Jobs::AssignedMoldableId)).equals((MoldableJobDescriptions::Table, MoldableJobDescriptions::Id)),
            )
            .inner_join(
                AssignedResources::Table,
                Expr::col((Jobs::Table, Jobs::AssignedMoldableId)).equals((AssignedResources::Table, AssignedResources::MoldableId)),
            )
            .and_where(Expr::col((Jobs::Table, Jobs::State)).eq("Running"))
            .and_where(Expr::col((WalltimeChange::Table, WalltimeChange::Pending)).ne(0))
            .order_by((Jobs::Table, Jobs::Id), sea_query::Order::Asc)
            .to_owned()
            .fetch_all(session)
            .await?;

        let mut jobs: Vec<WalltimeChangeJob> = Vec::new();
        let mut index: HashMap<i64, usize> = HashMap::new();
        for row in &rows {
            let job_id: i64 = row.get(Jobs::Id.unquoted());
            let rid: i32 = row.get(AssignedResources::ResourceId.unquoted());
            if let Some(&i) = index.get(&job_id) {
                jobs[i].rids.push(rid);
                continue;
            }
            let job = WalltimeChangeJob {
                job_id,
                queue_name: row.try_get(Jobs::QueueName.unquoted()).unwrap_or_default(),
                start_time: row.try_get(Jobs::StartTime.unquoted()).unwrap_or(0),
                user: row.try_get(Jobs::User.unquoted()).unwrap_or_default(),
                name: row.try_get(Jobs::Name.unquoted()).unwrap_or_default(),
                walltime: row.try_get(MoldableJobDescriptions::Walltime.unquoted()).unwrap_or(0),
                pending: row.try_get(WalltimeChange::Pending.unquoted()).unwrap_or(0),
                force: row.try_get(WalltimeChange::Force.unquoted()).unwrap_or_else(|_| "NO".to_string()),
                delay_next_jobs: row.try_get(WalltimeChange::DelayNextJobs.unquoted()).unwrap_or_else(|_| "NO".to_string()),
                granted: row.try_get(WalltimeChange::Granted.unquoted()).unwrap_or(0),
                granted_with_force: row.try_get(WalltimeChange::GrantedWithForce.unquoted()).unwrap_or(0),
                granted_with_delay_next_jobs: row.try_get(WalltimeChange::GrantedWithDelayNextJobs.unquoted()).unwrap_or(0),
                rids: vec![rid],
                types: HashMap::new(),
            };
            index.insert(job_id, jobs.len());
            jobs.push(job);
        }

        let ids: Vec<i64> = jobs.iter().map(|job| job.job_id).collect();
        let all_types = AllJobTypes::load_type_for_jobs(session, ids).await?;
        for job in jobs.iter_mut() {
            job.types = all_types
                .get_job_types(job.job_id)
                .into_iter()
                .map(|(k, v)| (k.to_string(), v.map(|s| s.to_string())))
                .collect();
        }
        Ok(jobs)
    })
}

/// Sum of the durations the job spent in the `Suspended` or `Resuming` state.
/// Mirrors `oar.lib.job_handling.get_job_suspended_sum_duration`.
pub fn get_job_suspended_sum_duration(session: &Session, job_id: i64, now: i64) -> Result<i64, Error> {
    session.runtime.block_on(async {
        let rows = Query::select()
            .columns(vec![JobStateLogs::DateStart, JobStateLogs::DateStop])
            .from(JobStateLogs::Table)
            .and_where(Expr::col(JobStateLogs::JobId).eq(job_id))
            .and_where(
                Expr::col(JobStateLogs::JobState).is_in(["Suspended".as_enum("job_state"), "Resuming".as_enum("job_state")]),
            )
            .to_owned()
            .fetch_all(session)
            .await?;
        let mut suspended_duration = 0i64;
        for row in rows {
            let date_start: i64 = row.try_get(JobStateLogs::DateStart.unquoted()).unwrap_or(0);
            let date_stop: i64 = row.try_get(JobStateLogs::DateStop.unquoted()).unwrap_or(0);
            let res_time = if date_stop == 0 { now - date_start } else { date_stop - date_start };
            if res_time > 0 {
                suspended_duration += res_time;
            }
        }
        Ok(suspended_duration)
    })
}

/// Returns `(start_time, moldable_walltime)` of a running job.
/// Mirrors `oar.lib.job_handling.get_running_job`.
pub fn get_running_job(session: &Session, job_id: i64) -> Result<Option<(i64, i64)>, Error> {
    session.runtime.block_on(async {
        let row = Query::select()
            .column((Jobs::Table, Jobs::StartTime))
            .column((MoldableJobDescriptions::Table, MoldableJobDescriptions::Walltime))
            .from(Jobs::Table)
            .inner_join(
                MoldableJobDescriptions::Table,
                Expr::col((Jobs::Table, Jobs::AssignedMoldableId)).equals((MoldableJobDescriptions::Table, MoldableJobDescriptions::Id)),
            )
            .and_where(Expr::col((Jobs::Table, Jobs::Id)).eq(job_id))
            .and_where(Expr::col((Jobs::Table, Jobs::State)).eq("Running"))
            .to_owned()
            .fetch_all(session)
            .await?;
        Ok(row.into_iter().next().map(|row| {
            (
                row.try_get(Jobs::StartTime.unquoted()).unwrap_or(0),
                row.try_get(MoldableJobDescriptions::Walltime.unquoted()).unwrap_or(0),
            )
        }))
    })
}

/// Update a walltime change request after processing.
/// `force` and `delay_next_jobs` must be `"YES"` or `"NO"`.
pub fn update_walltime_change_request(
    session: &Session,
    job_id: i64,
    pending: i64,
    force: &str,
    delay_next_jobs: &str,
    granted: i64,
    granted_with_force: i64,
    granted_with_delay_next_jobs: i64,
) -> Result<(), Error> {
    session.runtime.block_on(async {
        Query::update()
            .table(WalltimeChange::Table)
            .and_where(Expr::col(WalltimeChange::JobId).eq(job_id))
            .value(WalltimeChange::Pending, pending)
            .value(WalltimeChange::Force, force)
            .value(WalltimeChange::DelayNextJobs, delay_next_jobs)
            .value(WalltimeChange::Granted, granted)
            .value(WalltimeChange::GrantedWithForce, granted_with_force)
            .value(WalltimeChange::GrantedWithDelayNextJobs, granted_with_delay_next_jobs)
            .execute(session)
            .await?;
        Ok(())
    })
}

/// Insert (or replace) a walltime change request.
pub fn insert_walltime_change_request(session: &Session, job_id: i64, pending: i64, force: &str, delay_next_jobs: &str) -> Result<(), Error> {
    session.runtime.block_on(async {
        Query::delete()
            .from_table(WalltimeChange::Table)
            .and_where(Expr::col(WalltimeChange::JobId).eq(job_id))
            .execute(session)
            .await?;
        Query::insert()
            .into_table(WalltimeChange::Table)
            .columns(vec![
                WalltimeChange::JobId,
                WalltimeChange::Pending,
                WalltimeChange::Force,
                WalltimeChange::DelayNextJobs,
                WalltimeChange::Granted,
                WalltimeChange::GrantedWithForce,
                WalltimeChange::GrantedWithDelayNextJobs,
            ])
            .values_panic(vec![
                job_id.into(),
                pending.into(),
                force.into(),
                delay_next_jobs.into(),
                0.into(),
                0.into(),
                0.into(),
            ])
            .execute(session)
            .await?;
        Ok::<(), Error>(())
    })
}

/// Change the walltime of a job (its current moldable) and add a `WALLTIME` event.
/// Mirrors `oar.lib.job_handling.change_walltime`.
pub fn change_walltime(session: &Session, job_id: i64, new_walltime: i64, message: &str) -> Result<(), Error> {    session.runtime.block_on(async {
        Query::update()
            .table(MoldableJobDescriptions::Table)
            .and_where(Expr::col(MoldableJobDescriptions::JobId).eq(job_id))
            .value(MoldableJobDescriptions::Walltime, new_walltime)
            .execute(session)
            .await?;
        Ok::<(), Error>(())
    })?;
    crate::model::events::add_new_event_with_to_check(session, "WALLTIME", job_id, message, "NO");
    Ok(())
}

/// Returns the raw `type` strings of the given jobs, grouped by job id.
async fn load_raw_job_types(session: &Session, jobs: &[i64]) -> Result<HashMap<i64, Vec<String>>, Error> {
    use crate::model::job_types::JobTypes;
    if jobs.is_empty() {
        return Ok(HashMap::new());
    }
    let rows = Query::select()
        .columns(vec![JobTypes::JobId, JobTypes::Type])
        .from(JobTypes::Table)
        .and_where(Expr::col(JobTypes::JobId).is_in(jobs.iter().copied()))
        .to_owned()
        .fetch_all(session)
        .await?;
    let mut map: HashMap<i64, Vec<String>> = HashMap::new();
    for row in rows {
        let job_id: i64 = row.try_get(JobTypes::JobId.unquoted()).unwrap_or(0);
        let t: String = row.try_get(JobTypes::Type.unquoted()).unwrap_or_default();
        map.entry(job_id).or_default().push(t);
    }
    Ok(map)
}

/// Whether a predicted job sharing resources with our job must be ignored when
/// computing how far our job's walltime can be extended.
/// Mirrors the `exclude` clause of `get_possible_job_end_time_in_interval`.
fn candidate_is_excluded(
    our_types: &HashMap<String, Option<String>>,
    cand_raw_types: &[String],
    job_user: &str,
    job_name: &str,
    cand_user: &str,
    cand_name: &str,
) -> bool {
    let has = |t: &str| cand_raw_types.iter().any(|x| x == t);

    // The `allowed` (placeholder) case *replaces* the timesharing exclusions.
    if let Some(allowed) = our_types.get("allowed") {
        let allowed = allowed.as_deref().unwrap_or("");
        return has(&format!("placeholder={}", allowed));
    }

    if let Some(ts) = our_types.get("timesharing") {
        let ts = ts.as_deref().unwrap_or("");
        match ts {
            "user,*" | "*,user" => {
                return (has("timesharing=user,*") || has("timesharing=*,user")) && cand_user == job_user;
            }
            "name,*" | "*,name" => {
                return (has("timesharing=*,name") || has("timesharing=name,*")) && cand_name == job_name;
            }
            "name,user" | "user,name" => {
                return (has("timesharing=user,name") || has("timesharing=name,user")) && cand_name == job_name && cand_user == job_user;
            }
            "*,*" => return has("timesharing=*,*"),
            _ => {}
        }
    }
    false
}

/// Compute the possible end time of a job `[from_, to]` given the gantt of the
/// predicted jobs on `resources`.
/// Mirrors `oar.lib.job_handling.get_possible_job_end_time_in_interval`.
#[allow(clippy::too_many_arguments)]
pub fn get_possible_job_end_time_in_interval(
    session: &Session,
    from_: i64,
    to: i64,
    resources: &[i32],
    scheduler_job_security_time: i64,
    delay_next_jobs: &str,
    job_types: &HashMap<String, Option<String>>,
    job_user: &str,
    job_name: &str,
) -> Result<i64, Error> {
    session.runtime.block_on(async {
        if resources.is_empty() {
            return Ok(to);
        }
        let mut query = Query::select()
            .distinct()
            .columns(vec![
                (Jobs::Table, Jobs::Id),
                (Jobs::Table, Jobs::User),
                (Jobs::Table, Jobs::Name),
            ])
            .column((GanttJobsPredictions::Table, GanttJobsPredictions::StartTime))
            .from(GanttJobsResources::Table)
            .inner_join(
                GanttJobsPredictions::Table,
                Expr::col((GanttJobsResources::Table, GanttJobsResources::MoldableId))
                    .equals((GanttJobsPredictions::Table, GanttJobsPredictions::MoldableId)),
            )
            .inner_join(
                MoldableJobDescriptions::Table,
                Expr::col((GanttJobsPredictions::Table, GanttJobsPredictions::MoldableId))
                    .equals((MoldableJobDescriptions::Table, MoldableJobDescriptions::Id)),
            )
            .inner_join(
                Jobs::Table,
                Expr::col((MoldableJobDescriptions::Table, MoldableJobDescriptions::JobId)).equals((Jobs::Table, Jobs::Id)),
            )
            .and_where(Expr::col((GanttJobsResources::Table, GanttJobsResources::ResourceId)).is_in(resources.iter().copied()))
            .and_where(Expr::col((GanttJobsPredictions::Table, GanttJobsPredictions::StartTime)).gt(from_))
            .and_where(Expr::col((GanttJobsPredictions::Table, GanttJobsPredictions::StartTime)).lte(to))
            .to_owned();
        if delay_next_jobs == "YES" {
            query.and_where(Expr::col((Jobs::Table, Jobs::Reservation)).ne("None"));
        }
        let rows = query.fetch_all(session).await?;
        let cand_ids: Vec<i64> = rows.iter().map(|r| r.get::<i64, &str>(Jobs::Id.unquoted())).collect();
        let raw_types = load_raw_job_types(session, &cand_ids).await?;

        let mut first = to;
        for row in rows.iter() {
            let job_id: i64 = row.get(Jobs::Id.unquoted());
            let start_time: i64 = row.get(GanttJobsPredictions::StartTime.unquoted());
            let cand_user: String = row.try_get(Jobs::User.unquoted()).unwrap_or_default();
            let cand_name: String = row.try_get(Jobs::Name.unquoted()).unwrap_or_default();
            let empty = Vec::new();
            let cand_types = raw_types.get(&job_id).unwrap_or(&empty);
            // Besteffort jobs never block a walltime change.
            if cand_types.iter().any(|t| t == "besteffort") {
                continue;
            }
            if candidate_is_excluded(job_types, cand_types, job_user, job_name, &cand_user, &cand_name) {
                continue;
            }
            if first > (start_time - scheduler_job_security_time) {
                first = start_time - scheduler_job_security_time - 1;
            }
        }
        Ok(first)
    })
}

// ---------------------------------------------------------------------------
// Advance reservations waiting for their resources
// ---------------------------------------------------------------------------

#[derive(Iden)]
enum AliveResources {
    #[iden = "resources"]
    Table,
    #[iden = "resource_id"]
    ResourceId,
    #[iden = "state"]
    State,
}

/// Returns the database ids of the resources whose state is `Alive` or `Absent`.
/// Mirrors the `resource_set.roid_itvs` alive resource set.
pub fn get_alive_resource_ids(session: &Session) -> Result<Vec<i32>, Error> {
    get_resource_ids_with_states(session, &["alive", "absent"])
}

/// Returns the database ids of the resources whose state is `Absent`.
pub fn get_absent_resource_ids(session: &Session) -> Result<Vec<i32>, Error> {
    get_resource_ids_with_states(session, &["absent"])
}

fn get_resource_ids_with_states(session: &Session, wanted_states: &[&str]) -> Result<Vec<i32>, Error> {
    session.runtime.block_on(async {
        let rows = Query::select()
            .columns(vec![AliveResources::ResourceId, AliveResources::State])
            .from(AliveResources::Table)
            .to_owned()
            .fetch_all(session)
            .await?;
        Ok(rows
            .iter()
            .filter(|row| {
                let state: String = row.try_get(AliveResources::State.unquoted()).unwrap_or_default();
                let state = state.to_lowercase();
                wanted_states.iter().any(|wanted| state == *wanted)
            })
            .filter_map(|row| row.try_get::<i32, &str>(AliveResources::ResourceId.unquoted()).ok())
            .collect())
    })
}

/// Update the predicted start time of a job in the gantt.
/// Mirrors `oar.lib.job_handling.set_gantt_job_start_time`.
pub fn set_gantt_job_start_time(session: &Session, moldable_id: i64, start_time: i64) -> Result<(), Error> {
    session.runtime.block_on(async {
        Query::update()
            .table(GanttJobsPredictions::Table)
            .and_where(Expr::col(GanttJobsPredictions::MoldableId).eq(moldable_id))
            .value(GanttJobsPredictions::StartTime, start_time)
            .execute(session)
            .await?;
        Ok::<(), Error>(())
    })
}

/// Remove the given resources from the gantt assignment of a moldable.
/// Mirrors `oar.lib.job_handling.remove_gantt_resource_job`.
pub fn remove_gantt_resources(session: &Session, moldable_id: i64, resource_ids: &[i32]) -> Result<(), Error> {
    if resource_ids.is_empty() {
        return Ok(());
    }
    session.runtime.block_on(async {
        Query::delete()
            .from_table(GanttJobsResources::Table)
            .and_where(Expr::col(GanttJobsResources::MoldableId).eq(moldable_id))
            .and_where(Expr::col(GanttJobsResources::ResourceId).is_in(resource_ids.iter().copied()))
            .execute(session)
            .await?;
        Ok::<(), Error>(())
    })
}

// ---------------------------------------------------------------------------
// Energy saving (node sleep / wake up)
// ---------------------------------------------------------------------------

#[derive(Iden)]
enum NodeResources {
    #[iden = "resources"]
    Table,
    #[iden = "resource_id"]
    ResourceId,
    #[iden = "network_address"]
    NetworkAddress,
    #[iden = "type"]
    Type,
    #[iden = "state"]
    State,
    #[iden = "last_job_date"]
    LastJobDate,
    #[iden = "available_upto"]
    AvailableUpto,
    #[iden = "suspended_jobs"]
    SuspendedJobs,
}

/// Returns the nodes (`network_address`, `last_job_date`) that are idle at the
/// given date, i.e. alive default nodes with no gantt prediction before `date`.
/// Mirrors `oar.lib.node.search_idle_nodes`.
pub fn search_idle_nodes(session: &Session, date: i64) -> Result<Vec<(String, i64)>, Error> {
    session.runtime.block_on(async {
        use std::collections::HashSet;

        let busy_rows = Query::select()
            .distinct()
            .column((NodeResources::Table, NodeResources::NetworkAddress))
            .from(NodeResources::Table)
            .inner_join(
                GanttJobsResources::Table,
                Expr::col((GanttJobsResources::Table, GanttJobsResources::ResourceId))
                    .equals((NodeResources::Table, NodeResources::ResourceId)),
            )
            .inner_join(
                GanttJobsPredictions::Table,
                Expr::col((GanttJobsPredictions::Table, GanttJobsPredictions::MoldableId))
                    .equals((GanttJobsResources::Table, GanttJobsResources::MoldableId)),
            )
            .and_where(Expr::col((GanttJobsPredictions::Table, GanttJobsPredictions::StartTime)).lte(date))
            .and_where(Expr::col((NodeResources::Table, NodeResources::NetworkAddress)).ne(""))
            .and_where(Expr::col((NodeResources::Table, NodeResources::Type)).eq("default"))
            .to_owned()
            .fetch_all(session)
            .await?;
        let busy_nodes: HashSet<String> = busy_rows
            .iter()
            .filter_map(|row| row.try_get::<String, &str>(NodeResources::NetworkAddress.unquoted()).ok())
            .collect();

        let idle_rows = Query::select()
            .column((NodeResources::Table, NodeResources::NetworkAddress))
            .expr_as(Func::max(Expr::col((NodeResources::Table, NodeResources::LastJobDate))), Alias::new("last_job_date"))
            .from(NodeResources::Table)
            .and_where(Expr::col((NodeResources::Table, NodeResources::State)).eq("Alive"))
            .and_where(Expr::col((NodeResources::Table, NodeResources::NetworkAddress)).ne(""))
            .and_where(Expr::col((NodeResources::Table, NodeResources::Type)).eq("default"))
            .and_where(Expr::col((NodeResources::Table, NodeResources::AvailableUpto)).lt(2147483647i64))
            .and_where(Expr::col((NodeResources::Table, NodeResources::AvailableUpto)).gt(0))
            .group_by_col((NodeResources::Table, NodeResources::NetworkAddress))
            .to_owned()
            .fetch_all(session)
            .await?;

        Ok(idle_rows
            .iter()
            .filter_map(|row| {
                let hostname: String = row.try_get(NodeResources::NetworkAddress.unquoted()).ok()?;
                if busy_nodes.contains(&hostname) {
                    return None;
                }
                let last_job_date: i64 = row.try_get("last_job_date").unwrap_or(0);
                Some((hostname, last_job_date))
            })
            .collect())
    })
}

/// Returns the hostnames of the `Absent` nodes that must be woken up to launch
/// the jobs predicted before `date + wakeup_time`.
/// Mirrors `oar.lib.node.get_gantt_hostname_to_wake_up`.
pub fn get_gantt_hostname_to_wake_up(session: &Session, date: i64, wakeup_time: i64) -> Result<Vec<String>, Error> {
    session.runtime.block_on(async {
        let rows = Query::select()
            .column((NodeResources::Table, NodeResources::NetworkAddress))
            .from(NodeResources::Table)
            .inner_join(
                GanttJobsResources::Table,
                Expr::col((GanttJobsResources::Table, GanttJobsResources::ResourceId))
                    .equals((NodeResources::Table, NodeResources::ResourceId)),
            )
            .inner_join(
                GanttJobsPredictions::Table,
                Expr::col((GanttJobsPredictions::Table, GanttJobsPredictions::MoldableId))
                    .equals((GanttJobsResources::Table, GanttJobsResources::MoldableId)),
            )
            .inner_join(
                MoldableJobDescriptions::Table,
                Expr::col((GanttJobsPredictions::Table, GanttJobsPredictions::MoldableId))
                    .equals((MoldableJobDescriptions::Table, MoldableJobDescriptions::Id)),
            )
            .inner_join(
                Jobs::Table,
                Expr::col((MoldableJobDescriptions::Table, MoldableJobDescriptions::JobId)).equals((Jobs::Table, Jobs::Id)),
            )
            .and_where(Expr::col((GanttJobsPredictions::Table, GanttJobsPredictions::StartTime)).lte(date + wakeup_time))
            .and_where(Expr::col((Jobs::Table, Jobs::State)).eq("Waiting"))
            .and_where(Expr::col((NodeResources::Table, NodeResources::State)).eq("Absent"))
            .and_where(Expr::col((NodeResources::Table, NodeResources::NetworkAddress)).ne(""))
            .and_where(Expr::col((NodeResources::Table, NodeResources::Type)).eq("default"))
            .and_where(
                Expr::col((GanttJobsPredictions::Table, GanttJobsPredictions::StartTime))
                    .add(Expr::col((MoldableJobDescriptions::Table, MoldableJobDescriptions::Walltime)))
                    .lte(Expr::col((NodeResources::Table, NodeResources::AvailableUpto))),
            )
            .group_by_col((NodeResources::Table, NodeResources::NetworkAddress))
            .to_owned()
            .fetch_all(session)
            .await?;
        Ok(rows
            .iter()
            .filter_map(|row| row.try_get::<String, &str>(NodeResources::NetworkAddress.unquoted()).ok())
            .collect())
    })
}

/// Returns the date of the next job predicted on the given node.
/// Mirrors `oar.lib.node.get_next_job_date_on_node`.
pub fn get_next_job_date_on_node(session: &Session, hostname: &str) -> Result<Option<i64>, Error> {
    session.runtime.block_on(async {
        let rows = Query::select()
            .expr_as(Func::min(Expr::col((GanttJobsPredictions::Table, GanttJobsPredictions::StartTime))), Alias::new("next_job_date"))
            .from(NodeResources::Table)
            .inner_join(
                GanttJobsResources::Table,
                Expr::col((GanttJobsResources::Table, GanttJobsResources::ResourceId))
                    .equals((NodeResources::Table, NodeResources::ResourceId)),
            )
            .inner_join(
                GanttJobsPredictions::Table,
                Expr::col((GanttJobsPredictions::Table, GanttJobsPredictions::MoldableId))
                    .equals((GanttJobsResources::Table, GanttJobsResources::MoldableId)),
            )
            .and_where(Expr::col((NodeResources::Table, NodeResources::NetworkAddress)).eq(hostname))
            .to_owned()
            .fetch_all(session)
            .await?;
        Ok(rows.first().and_then(|row| row.try_get::<i64, &str>("next_job_date").ok()))
    })
}

/// Returns the last date the given node was woken up (`WAKEUP_NODE` event).
/// Mirrors `oar.lib.node.get_last_wake_up_date_of_node`.
pub fn get_last_wake_up_date_of_node(session: &Session, hostname: &str) -> Result<Option<i64>, Error> {
    session.runtime.block_on(async {
        let rows = Query::select()
            .expr_as(Func::max(Expr::col((EventLogs::Table, EventLogs::Date))), Alias::new("last_wake_up_date"))
            .from(EventLogs::Table)
            .inner_join(
                EventLogHostnames::Table,
                Expr::col((EventLogHostnames::Table, EventLogHostnames::EventId)).equals((EventLogs::Table, EventLogs::EventId)),
            )
            .and_where(Expr::col((EventLogHostnames::Table, EventLogHostnames::Hostname)).eq(hostname))
            .and_where(Expr::col((EventLogs::Table, EventLogs::Type)).eq("WAKEUP_NODE"))
            .to_owned()
            .fetch_all(session)
            .await?;
        Ok(rows.first().and_then(|row| row.try_get::<i64, &str>("last_wake_up_date").ok()))
    })
}

/// Update the energy-saving related columns of a resource.
pub fn set_resource_energy_fields(
    session: &Session,
    resource_id: i32,
    state: Option<&str>,
    available_upto: Option<i64>,
    last_job_date: Option<i64>,
) -> Result<(), Error> {
    session.runtime.block_on(async {
        let mut query = Query::update();
        query.table(NodeResources::Table).and_where(Expr::col(NodeResources::ResourceId).eq(resource_id));
        if let Some(state) = state {
            query.value(NodeResources::State, state);
        }
        if let Some(available_upto) = available_upto {
            query.value(NodeResources::AvailableUpto, available_upto);
        }
        if let Some(last_job_date) = last_job_date {
            query.value(NodeResources::LastJobDate, last_job_date);
        }
        query.execute(session).await?;
        Ok::<(), Error>(())
    })
}

/// Set the `last_job_date` of the resources assigned to a moldable.
/// Async variant usable from within another `block_on` (e.g. `Job::set_state`).
/// Mirrors `oar.lib.job_handling.update_scheduler_last_job_date`.
pub(crate) async fn update_scheduler_last_job_date_async(session: &Session, date: i64, moldable_id: i64) -> Result<(), Error> {
    let subquery = Query::select()
        .column(AssignedResources::ResourceId)
        .from(AssignedResources::Table)
        .and_where(Expr::col(AssignedResources::MoldableId).eq(moldable_id))
        .to_owned();
    Query::update()
        .table(NodeResources::Table)
        .and_where(Expr::col(NodeResources::ResourceId).in_subquery(subquery))
        .value(NodeResources::LastJobDate, date)
        .execute(session)
        .await?;
    Ok(())
}

/// Returns the state log entries of a job as `(state, date_start, date_stop)`.
pub fn get_job_state_logs(session: &Session, job_id: i64) -> Result<Vec<(String, i64, i64)>, Error> {
    session.runtime.block_on(async {
        let rows = Query::select()
            .columns(vec![JobStateLogs::JobState, JobStateLogs::DateStart, JobStateLogs::DateStop])
            .from(JobStateLogs::Table)
            .and_where(Expr::col(JobStateLogs::JobId).eq(job_id))
            .order_by(JobStateLogs::Id, sea_query::Order::Asc)
            .to_owned()
            .fetch_all(session)
            .await?;
        Ok(rows
            .iter()
            .map(|row| {
                (
                    row.try_get::<String, &str>(JobStateLogs::JobState.unquoted()).unwrap_or_default(),
                    row.try_get::<i64, &str>(JobStateLogs::DateStart.unquoted()).unwrap_or(0),
                    row.try_get::<i64, &str>(JobStateLogs::DateStop.unquoted()).unwrap_or(0),
                )
            })
            .collect())
    })
}

/// Returns the `last_job_date` of a resource.
pub fn get_resource_last_job_date(session: &Session, resource_id: i32) -> Result<Option<i64>, Error> {
    session.runtime.block_on(async {
        let rows = Query::select()
            .column((NodeResources::Table, NodeResources::LastJobDate))
            .from(NodeResources::Table)
            .and_where(Expr::col((NodeResources::Table, NodeResources::ResourceId)).eq(resource_id))
            .to_owned()
            .fetch_all(session)
            .await?;
        Ok(rows
            .first()
            .and_then(|row| row.try_get::<i64, &str>(NodeResources::LastJobDate.unquoted()).ok()))
    })
}

// ---------------------------------------------------------------------------
// Suspend / resume
// ---------------------------------------------------------------------------

/// Returns the ids of the other active jobs sharing at least one resource with
/// the given moldable. Mirrors `oar.lib.job_handling.get_jobs_on_resuming_job_resources`.
pub fn get_jobs_on_resuming_job_resources(session: &Session, job_id: i64, moldable_id: i64) -> Result<Vec<i64>, Error> {
    session.runtime.block_on(async {
        let states = ["toLaunch", "toError", "toAckReservation", "Launching", "Running", "Finishing"];
        let resource_subquery = Query::select()
            .column(AssignedResources::ResourceId)
            .from(AssignedResources::Table)
            .and_where(Expr::col(AssignedResources::MoldableId).eq(moldable_id))
            .and_where(Expr::col(AssignedResources::Index).eq("CURRENT"))
            .to_owned();
        let rows = Query::select()
            .distinct()
            .column((Jobs::Table, Jobs::Id))
            .from(Jobs::Table)
            .inner_join(
                AssignedResources::Table,
                Expr::col((AssignedResources::Table, AssignedResources::MoldableId))
                    .equals((Jobs::Table, Jobs::AssignedMoldableId)),
            )
            .and_where(Expr::col((AssignedResources::Table, AssignedResources::Index)).eq("CURRENT"))
            .and_where(Expr::col((AssignedResources::Table, AssignedResources::ResourceId)).in_subquery(resource_subquery))
            .and_where(Expr::col((Jobs::Table, Jobs::Id)).ne(job_id))
            .and_where(Expr::col((Jobs::Table, Jobs::State)).is_in(states.iter().map(|s| s.as_enum("job_state"))))
            .to_owned()
            .fetch_all(session)
            .await?;
        Ok(rows.iter().filter_map(|row| row.try_get::<i64, &str>(Jobs::Id.unquoted()).ok()).collect())
    })
}

/// Returns the resources that currently host a suspended job
/// (`resources.suspended_jobs == 'YES'`).
pub fn get_current_resources_with_suspended_job(session: &Session) -> Result<Vec<i32>, Error> {
    session.runtime.block_on(async {
        let rows = Query::select()
            .column((NodeResources::Table, NodeResources::ResourceId))
            .from(NodeResources::Table)
            .and_where(Expr::col((NodeResources::Table, NodeResources::SuspendedJobs)).eq("YES"))
            .to_owned()
            .fetch_all(session)
            .await?;
        Ok(rows
            .iter()
            .filter_map(|row| row.try_get::<i32, &str>(NodeResources::ResourceId.unquoted()).ok())
            .collect())
    })
}

/// Reset `resources.suspended_jobs` to `NO`, except for the resources that still
/// host a suspended job. Mirrors the resource update of `resume_job_action`.
pub fn clear_suspended_jobs(session: &Session) -> Result<(), Error> {
    let still_suspended = get_current_resources_with_suspended_job(session)?;
    session.runtime.block_on(async {
        let mut query = Query::update();
        query.table(NodeResources::Table).value(NodeResources::SuspendedJobs, "NO");
        if !still_suspended.is_empty() {
            query.and_where(Expr::col(NodeResources::ResourceId).is_not_in(still_suspended.iter().copied()));
        }
        query.execute(session).await?;
        Ok::<(), Error>(())
    })
}
