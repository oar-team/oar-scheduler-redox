use crate::model::gantt::{JobResourceDescriptions, JobResourceGroups, MoldableJobDescriptions};
use crate::{Session, SessionInsertStatement};
use sea_query::Iden;
use sea_query::{Alias, Expr, Query};
use sqlx::{Error, Row};

// jobs and related tables
#[derive(Iden)]
pub enum Jobs {
    #[iden = "jobs"]
    Table,
    #[iden = "job_id"]
    JobId,
    #[iden = "job_name"]
    JobName,
    #[iden = "job_env"]
    JobEnv,
    #[iden = "cpuset"]
    CpuSet,
    #[iden = "job_type"]
    JobType,
    #[iden = "info_type"]
    InfoType,
    #[iden = "state"]
    State,
    #[iden = "reservation"]
    Reservation,
    #[iden = "message"]
    Message,
    #[iden = "job_user"]
    JobUser,
    #[iden = "project"]
    Project,
    #[iden = "job_group"]
    JobGroup,
    #[iden = "command"]
    Command,
    #[iden = "exit_code"]
    ExitCode,
    #[iden = "queue_name"]
    QueueName,
    #[iden = "properties"]
    Properties,
    #[iden = "launching_directory"]
    LaunchingDirectory,
    #[iden = "submission_time"]
    SubmissionTime,
    #[iden = "start_time"]
    StartTime,
    #[iden = "stop_time"]
    StopTime,
    #[iden = "file_id"]
    FileId,
    #[iden = "accounted"]
    Accounted,
    #[iden = "checkpoint"]
    Checkpoint,
    #[iden = "checkpoint_signal"]
    CheckpointSignal,
    #[iden = "notify"]
    Notify,
    #[iden = "assigned_moldable_job"]
    AssignedMoldableJob,
    #[iden = "stdout_file"]
    StdoutFile,
    #[iden = "stderr_file"]
    StderrFile,
    #[iden = "resubmit_job_id"]
    ResubmitJobId,
    #[iden = "suspended"]
    Suspended,
    #[iden = "array_id"]
    ArrayId,
    #[iden = "initial_request"]
    InitialRequest,
    #[iden = "scheduler_info"]
    SchedulerInfo,
}

#[derive(Iden)]
pub enum JobStateLogs {
    #[iden = "job_state_logs"]
    Table,
    #[iden = "job_state_log_id"]
    JobStateLogId,
    #[iden = "job_id"]
    JobId,
    #[iden = "job_state"]
    JobState,
    #[iden = "date_start"]
    DateStart,
    #[iden = "date_stop"]
    DateStop,
}

#[derive(Iden)]
pub enum FragJobs {
    #[iden = "frag_jobs"]
    Table,
    #[iden = "frag_id_job"]
    FragIdJob,
    #[iden = "frag_date"]
    FragDate,
    #[iden = "frag_state"]
    FragState,
}

#[derive(Iden)]
pub enum Challenges {
    #[iden = "challenges"]
    Table,
    #[iden = "job_id"]
    JobId,
    #[iden = "challenge"]
    Challenge,
    #[iden = "ssh_private_key"]
    SshPrivateKey,
    #[iden = "ssh_public_key"]
    SshPublicKey,
}

#[derive(Iden)]
pub enum JobTypes {
    #[iden = "job_types"]
    Table,
    #[iden = "job_type_id"]
    JobTypeId,
    #[iden = "job_id"]
    JobId,
    #[iden = "type"]
    Type,
}

#[derive(Iden)]
pub enum JobDependencies {
    #[iden = "job_dependencies"]
    Table,
    #[iden = "job_id"]
    JobId,
    #[iden = "job_id_required"]
    JobIdRequired,
    #[iden = "job_dependency_index"]
    JobDependencyIndex,
}

/// Struct to insert a new job with related entries into the database.
/// Only used by tests.
///
/// Defaults applied (if not provided):
/// - queue_name: "default"
/// - launching_directory: "" (internal default)
/// - checkpoint_signal: 0 (internal default)
/// - properties: "" (internal default)
/// - res: vec![(60, vec![("resource_id=1".to_string(), "".to_string())])]
/// - types: []
///
/// Behavior:
/// - Insert into `jobs` with mapped fields (user -> job_user).
/// - For each res entry (walltime, groups):
///   - insert into `moldable_job_descriptions` (moldable_job_id, moldable_walltime).
///   - For each group: insert `job_resource_groups` with (res_group_moldable_id, res_group_property).
///   - For each resource description in the group ("k=v" split by "/"): insert `job_resource_descriptions` with order preserved.
/// - Insert job types into `job_types`.
///
/// Return: job_id
pub struct NewJob {
    pub user: Option<String>, // mapped to jobs.job_user
    pub queue_name: Option<String>,
    /// res = [(walltime, [("res_hierarchy", "properties_sql"), ...]), ...]
    pub res: Vec<(i64, Vec<(String, String)>)>,
    pub types: Option<Vec<String>>,
}

impl NewJob {
    pub fn insert(&self, session: &Session) -> Result<i64, Error> {
        session.runtime.block_on(async { self.insert_async(session).await })
    }
    async fn insert_async(&self, session: &Session) -> Result<i64, Error> {
        // Apply defaults
        let launching_directory = "".to_string();
        let checkpoint_signal: i64 = 0;
        let properties = "".to_string();
        let queue_name = self.queue_name.clone().unwrap_or_else(|| "default".to_string());
        let job_user = self.user.clone().unwrap_or_else(|| "".to_string());

        let types: Vec<String> = self.types.clone().unwrap_or_default();

        // Insert job
        let row = Query::insert()
            .into_table(Jobs::Table)
            .columns(vec![
                Alias::new(Jobs::LaunchingDirectory.to_string()),
                Alias::new(Jobs::CheckpointSignal.to_string()),
                Alias::new(Jobs::Properties.to_string()),
                Alias::new(Jobs::QueueName.to_string()),
                Alias::new(Jobs::JobUser.to_string()),
            ])
            .values_panic(vec![
                Expr::val(&launching_directory),
                Expr::val(checkpoint_signal),
                Expr::val(&properties),
                Expr::val(&queue_name),
                Expr::val(&job_user),
            ])
            .returning_col(Jobs::JobId)
            .fetch_one(session)
            .await?;
        let job_id: i64 = row.try_get(0)?;

        // Insert moldable_job_descriptions, job_resource_groups, job_resource_descriptions
        let mut created_moldable_ids: Vec<i64> = Vec::new();
        for (walltime, groups) in self.res.iter() {
            // moldable_job_descriptions
            let mld_row = Query::insert()
                .into_table(MoldableJobDescriptions::Table)
                .columns(vec![
                    Alias::new(MoldableJobDescriptions::MoldableJobId.to_string()),
                    Alias::new(MoldableJobDescriptions::MoldableWalltime.to_string()),
                ])
                .values_panic(vec![Expr::val(job_id), Expr::val(*walltime)])
                .returning_col(MoldableJobDescriptions::MoldableId)
                .fetch_one(session)
                .await?;
            let moldable_id: i64 = mld_row.try_get(0)?;
            created_moldable_ids.push(moldable_id);

            // job_resource_groups for each group
            for (res_hierarchy, prop_sql) in groups.iter() {
                let grp_row = Query::insert()
                    .into_table(JobResourceGroups::Table)
                    .columns(vec![
                        Alias::new(JobResourceGroups::ResGroupMoldableId.to_string()),
                        Alias::new(JobResourceGroups::ResGroupProperty.to_string()),
                    ])
                    .values_panic(vec![Expr::val(moldable_id), Expr::val(prop_sql)])
                    .returning_col(JobResourceGroups::ResGroupId)
                    .fetch_one(session)
                    .await?;
                let group_id: i64 = grp_row.try_get(0)?;

                // job_resource_descriptions for each k=v in order
                for (idx, kv) in res_hierarchy.split('/').enumerate() {
                    if kv.trim().is_empty() {
                        continue;
                    }
                    let mut it = kv.splitn(2, '=');
                    let k = it.next().unwrap_or("");
                    let v = it.next().unwrap_or("");
                    Query::insert()
                        .into_table(JobResourceDescriptions::Table)
                        .columns(vec![
                            Alias::new(JobResourceDescriptions::ResJobGroupId.to_string()),
                            Alias::new(JobResourceDescriptions::ResJobResourceType.to_string()),
                            Alias::new(JobResourceDescriptions::ResJobValue.to_string()),
                            Alias::new(JobResourceDescriptions::ResJobOrder.to_string()),
                        ])
                        .values_panic(vec![
                            Expr::val(group_id),
                            Expr::val(k),
                            // In DB schema SQLite, res_job_value is INTEGER; keep behavior by parsing else default 0
                            match v.parse::<i64>() {
                                Ok(i) => Expr::val(i),
                                Err(_) => Expr::val(0),
                            },
                            Expr::val(idx as i64),
                        ])
                        .execute(session)
                        .await?;
                }
            }
        }

        // job_types
        if !types.is_empty() {
            for typ in types.iter() {
                Query::insert()
                    .into_table(JobTypes::Table)
                    .columns(vec![Alias::new(JobTypes::JobId.to_string()), Alias::new(JobTypes::Type.to_string())])
                    .values_panic(vec![Expr::val(job_id), Expr::val(typ)])
                    .execute(session)
                    .await?;
            }
        }

        // let _ = created_moldable_ids; // currently not returned
        Ok(job_id)
    }
}
