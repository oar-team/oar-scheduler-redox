/*
 * Copyright (c) 2025 Clément GRENNERAT
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the
 * GNU General Public License as published by the Free Software Foundation, version 3.
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU General Public License for more details.
 * You should have received a copy of the GNU General Public License along with this program.
 * If not, see https://www.gnu.org/licenses/.
 *
 */
use crate::model::job_dependencies::AllJobDependencies;
use crate::model::job_types::{AllJobTypes, JobTypes};
use crate::model::moldable::{AllJobMoldables, JobResourceDescriptions, JobResourceGroups, MoldableJobDescriptions};
use crate::model::{GanttJobsPredictions, SqlEnum};
use crate::{Session, SessionInsertStatement, SessionSelectStatement, SessionUpdateStatement};
use indexmap::IndexMap;
use log::{debug, info, warn};
use oar_scheduler_core::model::job::JobBuilder;
use oar_scheduler_core::platform::Job;
use sea_query::{Alias, Expr, Query};
use sea_query::{ExprTrait, Iden};
use sqlx::{Error, Row};
use std::io::{stdout, Write};

// jobs and related tables
#[derive(Iden)]
pub enum Jobs {
    #[iden = "jobs"]
    Table,
    #[iden = "job_id"]
    Id,
    #[iden = "job_name"]
    Name,
    #[iden = "job_env"]
    Env,
    #[iden = "cpuset"]
    CpuSet,
    #[iden = "job_type"]
    Type,
    #[iden = "info_type"]
    InfoType,
    #[iden = "state"]
    State,
    #[iden = "reservation"]
    Reservation,
    #[iden = "message"]
    Message,
    #[iden = "job_user"]
    User,
    #[iden = "project"]
    Project,
    #[iden = "job_group"]
    Group,
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
    AssignedMoldableId,
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

pub enum JobState {
    ToLaunch,
    ToError,
    ToAckReservation,
    Launching,
    Running,
    Finishing,
    Waiting,
    Hold,
    Suspended,
    Resuming,
}
impl SqlEnum for JobState {
    fn as_str(&self) -> &str {
        match self {
            JobState::ToLaunch => "toLaunch",
            JobState::ToError => "toError",
            JobState::ToAckReservation => "toAckReservation",
            JobState::Launching => "Launching",
            JobState::Running => "Running",
            JobState::Finishing => "Finishing",
            JobState::Waiting => "Waiting",
            JobState::Hold => "Hold",
            JobState::Suspended => "Suspended",
            JobState::Resuming => "Resuming",
        }
    }

    fn from_str(s: &str) -> Option<Self>
    where
        Self: Sized,
    {
        match s {
            "toLaunch" => Some(JobState::ToLaunch),
            "toError" => Some(JobState::ToError),
            "toAckReservation" => Some(JobState::ToAckReservation),
            "Launching" => Some(JobState::Launching),
            "Running" => Some(JobState::Running),
            "Finishing" => Some(JobState::Finishing),
            "Waiting" => Some(JobState::Waiting),
            "Hold" => Some(JobState::Hold),
            "Suspended" => Some(JobState::Suspended),
            "Resuming" => Some(JobState::Resuming),
            _ => None,
        }
    }
}

pub enum JobReservation {
    ToSchedule,
    Scheduled,
    None,
    Error,
}
impl SqlEnum for JobReservation {
    fn as_str(&self) -> &str {
        match self {
            JobReservation::ToSchedule => "toSchedule",
            JobReservation::Scheduled => "Scheduled",
            JobReservation::None => "None",
            JobReservation::Error => "Error",
        }
    }
    fn from_str(s: &str) -> Option<Self>
    where
        Self: Sized,
    {
        match s {
            "toSchedule" => Some(JobReservation::ToSchedule),
            "Scheduled" => Some(JobReservation::Scheduled),
            "None" => Some(JobReservation::None),
            "Error" => Some(JobReservation::Error),
            _ => None,
        }
    }
}

#[derive(Iden)]
pub enum JobStateLogs {
    #[iden = "job_state_logs"]
    Table,
    #[iden = "job_state_log_id"]
    Id,
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

pub trait JobDatabaseRequests {
    fn get_jobs(
        session: &Session,
        queues: Option<Vec<String>>,
        reservation: Option<JobReservation>,
        states: Option<Vec<JobState>>,
    ) -> Result<IndexMap<i64, Job>, Error>;
    fn get_gantt_jobs(
        session: &Session,
        queues: Option<Vec<String>>,
        reservation: Option<JobReservation>,
        states: Option<Vec<JobState>>,
    ) -> Result<Vec<Job>, Error>;
    fn set_state(&self, session: &Session, new_state: &str) -> Result<(), Error>;
    fn set_message(&self, session: &Session, message: &str) -> Result<(), Error>;
    fn set_resa_state(&self, session: &Session, new_resa_state: &str) -> Result<(), Error>;
}

impl JobDatabaseRequests for Job {
    /// Get jobs from the database.
    /// If `queues` is `Some`, only jobs in these queues are returned.
    /// If `reservation` is `Some`, only jobs in this reservation value are returned.
    /// If `state` is `Some`, only jobs in one of the state are returned.
    /// Jobs are always ordered by start time and then by job id ascending, making the waiting jobs to be ordered by submission time, and scheduled/AR jobs by start time.
    ///
    /// Inside queues_schedule, scheduled jobs resources and start time should be fetched from the `gantt_jobs_resources` and `gantt_jobs_prediction` tables
    /// and not from the `assigned_resources` table and `start_time` columns of the `jobs` table. Look at this function: `get_gantt_scheduled_jobs`.
    fn get_jobs(
        session: &Session,
        queues: Option<Vec<String>>,
        reservation: Option<JobReservation>,
        states: Option<Vec<JobState>>,
    ) -> Result<IndexMap<i64, Job>, Error> {
        let jobs = session.runtime.block_on(async {
            let rows = Query::select()
                .columns(vec![
                    Jobs::Id,
                    Jobs::Name,
                    Jobs::User,
                    Jobs::Project,
                    Jobs::QueueName,
                    Jobs::SubmissionTime,
                    Jobs::StartTime,
                    Jobs::StopTime,
                    Jobs::State,
                    Jobs::Reservation,
                    Jobs::AssignedMoldableId,
                ])
                .from(Jobs::Table)
                .apply_if(queues, |req, queues| {
                    req.and_where(Expr::col(Jobs::QueueName).is_in(queues));
                })
                .apply_if(reservation, |req, reservation| {
                    req.and_where(Expr::col(Jobs::Reservation).eq(reservation.as_str()));
                })
                .apply_if(states, |req, states| {
                    req.and_where(Expr::col(Jobs::State).is_in(states.iter().map(|s| s.as_str()).collect::<Vec<&str>>()));
                })
                .order_by(Jobs::StartTime, sea_query::Order::Asc)
                .order_by(Jobs::Id, sea_query::Order::Asc)
                .to_owned()
                .fetch_all(session)
                .await?;

            let job_ids = rows.iter().map(|r| r.get::<i64, &str>(Jobs::Id.unquoted())).collect::<Vec<i64>>();

            let jobs_types = AllJobTypes::load_type_for_jobs(session, job_ids.clone()).await?;
            let jobs_dependencies = AllJobDependencies::load_dependencies_for_jobs(session, job_ids.clone()).await?;
            let jobs_moldables = AllJobMoldables::load_moldables_for_jobs(session, job_ids).await?;

            let mut jobs = IndexMap::new();
            for row in rows {
                let id: i64 = row.get(Jobs::Id.unquoted());
                let moldables = jobs_moldables.get_job_moldables(id);

                let mut job_builder = JobBuilder::new(id)
                    .types(jobs_types.get_job_types(id))
                    .name_opt(row.try_get(Jobs::Name.unquoted()).map(|s: String| s.into_boxed_str()).ok())
                    .user_opt(row.try_get(Jobs::User.unquoted()).map(|s: String| s.into_boxed_str()).ok())
                    .project_opt(row.try_get(Jobs::Project.unquoted()).map(|s: String| s.into_boxed_str()).ok())
                    .queue(row.get::<String, &str>(Jobs::QueueName.unquoted()).into_boxed_str())
                    .dependencies(jobs_dependencies.get_job_dependencies(id))
                    .submission_time(row.get::<i64, &str>(Jobs::SubmissionTime.unquoted()))
                    .assign_opt(jobs_moldables.get_job_assignment(session, &row, false).await)
                    .moldables(moldables);
                // Reservation jobs
                if JobReservation::ToSchedule.as_str() == row.get::<String, &str>(Jobs::Reservation.unquoted()) {
                    job_builder = job_builder.set_advance_reservation_start_time(row.get::<i64, &str>(Jobs::StartTime.unquoted()));
                };
                jobs.insert(id, job_builder.build());
            }
            Ok::<IndexMap<i64, Job>, Error>(jobs)
        })?;
        Ok(jobs)
    }

    /// Get jobs from the database, taking their assignments data from the gantt tables `gantt_jobs_resources` and `gantt_jobs_prediction`.
    /// If `queues` is `Some`, only jobs in these queues are returned.
    /// If `reservation` is `Some`, only jobs in this reservation value are returned.
    /// If `state` is `Some`, only jobs in one of the state are returned.
    /// Jobs are always ordered by start time and then by job id ascending, making the waiting jobs to be ordered by submission time, and scheduled/AR jobs by start time.
    fn get_gantt_jobs(
        session: &Session,
        queues: Option<Vec<String>>,
        reservation: Option<JobReservation>,
        states: Option<Vec<JobState>>,
    ) -> Result<Vec<Job>, Error> {
        session.runtime.block_on(async {
            let rows = Query::select()
                .columns(vec![
                    Jobs::Id.unquoted(),
                    Jobs::Name.unquoted(),
                    Jobs::User.unquoted(),
                    Jobs::Project.unquoted(),
                    Jobs::QueueName.unquoted(),
                    Jobs::SubmissionTime.unquoted(),
                    GanttJobsPredictions::StartTime.unquoted(),
                    Jobs::State.unquoted(),
                    Jobs::Reservation.unquoted(),
                    Jobs::AssignedMoldableId.unquoted(),
                ])
                .from(Jobs::Table)
                .inner_join(
                    GanttJobsPredictions::Table,
                    Expr::col(Jobs::AssignedMoldableId).equals(GanttJobsPredictions::MoldableId),
                )
                .apply_if(reservation, |req, reservation| {
                    req.and_where(Expr::col(Jobs::Reservation).eq(reservation.as_str()));
                })
                .apply_if(queues, |req, queues| {
                    req.and_where(Expr::col(Jobs::QueueName).is_in(queues));
                })
                .apply_if(states, |req, states| {
                    req.and_where(Expr::col(Jobs::State).is_in(states.iter().map(|s| s.as_str()).collect::<Vec<&str>>()));
                })
                .order_by(GanttJobsPredictions::StartTime, sea_query::Order::Asc)
                .order_by(Jobs::Id, sea_query::Order::Asc)
                .to_owned()
                .fetch_all(session)
                .await?;

            let job_ids = rows.iter().map(|r| r.get::<i64, &str>(Jobs::Id.unquoted())).collect::<Vec<i64>>();

            let jobs_types = AllJobTypes::load_type_for_jobs(session, job_ids.clone()).await?;
            let jobs_dependencies = AllJobDependencies::load_dependencies_for_jobs(session, job_ids.clone()).await?;
            let jobs_moldables = AllJobMoldables::load_moldables_for_jobs(session, job_ids).await?;

            let mut jobs: Vec<Job> = Vec::with_capacity(rows.len());
            for row in rows {
                let id: i64 = row.get(Jobs::Id.unquoted());
                let moldables = jobs_moldables.get_job_moldables(id);

                let mut job_builder = JobBuilder::new(id)
                    .types(jobs_types.get_job_types(id))
                    .name_opt(row.try_get(Jobs::Name.unquoted()).map(|s: String| s.into_boxed_str()).ok())
                    .user_opt(row.try_get(Jobs::User.unquoted()).map(|s: String| s.into_boxed_str()).ok())
                    .project_opt(row.try_get(Jobs::Project.unquoted()).map(|s: String| s.into_boxed_str()).ok())
                    .queue(row.get::<String, &str>(Jobs::QueueName.unquoted()).into_boxed_str())
                    .dependencies(jobs_dependencies.get_job_dependencies(id))
                    .submission_time(row.get::<i64, &str>(Jobs::SubmissionTime.unquoted()))
                    .assign_opt(jobs_moldables.get_job_assignment(session, &row, true).await)
                    .moldables(moldables);
                // Reservation jobs
                if JobReservation::ToSchedule.as_str() == row.get::<String, &str>(Jobs::Reservation.unquoted()) {
                    job_builder = job_builder.set_advance_reservation_start_time(row.get::<i64, &str>(Jobs::StartTime.unquoted()));
                };
                jobs.push(job_builder.build());
            }
            Ok(jobs)
        })
    }

    fn set_state(&self, session: &Session, new_state: &str) -> Result<(), Error> {
        session.runtime.block_on(async {
            let tx = session.begin().await;
            let mut states = vec![
                "toLaunch",
                "toError",
                "toAckReservation",
                "Launching",
                "Running",
                "Finishing",
                "Waiting",
                "Hold",
                "Suspended",
                "Resuming",
            ];
            states.remove(states.iter().position(|s| *s == new_state).expect("Invalid state"));
            let res = Query::update()
                .table(Jobs::Table)
                .and_where(Expr::col(Jobs::Id).eq(self.id))
                .and_where(Expr::col(Jobs::State).is_in(states))
                .value(Jobs::State, new_state)
                .execute(session)
                .await?;
            tx.commit().await.unwrap();
            if res == 0 {
                warn!(
                    "Job is already terminated or in error or wanted state, job_id: {}, wanted state: {}",
                    self.id, new_state
                );
                return Ok(());
            }

            debug!("Job {} state changed to {}", self.id, new_state);

            // TODO: update the JobStateLog table and notify user as done here:
            //   https://github.com/oar-team/oar3/blob/e6b6e7e59eb751cc2e7388d6c2fb7f94a3ac8c6e/oar/lib/job_handling.py#L1714-L1800

            Ok(())
        })
    }

    fn set_message(&self, session: &Session, message: &str) -> Result<(), Error> {
        session.runtime.block_on(async {
            let res = Query::update()
                .table(Jobs::Table)
                .and_where(Expr::col(Jobs::Id).eq(self.id))
                .value(Jobs::Message, message)
                .execute(session)
                .await?;
            if res == 0 {
                warn!("Job not found when setting message, job_id: {}, message: {}", self.id, message);
            }
            Ok(())
        })
    }

    fn set_resa_state(&self, session: &Session, new_resa_state: &str) -> Result<(), Error> {
        session.runtime.block_on(async {
            let res = Query::update()
                .table(Jobs::Table)
                .and_where(Expr::col(Jobs::Id).eq(self.id))
                .value(Jobs::Reservation, new_resa_state)
                .execute(session)
                .await?;
            if res == 0 {
                warn!(
                    "Job not found when setting reservation state, job_id: {}, reservation state: {}",
                    self.id, new_resa_state
                );
            }
            Ok(())
        })
    }
}

pub struct NewJob {
    pub user: Option<String>, // jobs.job_user
    pub queue_name: String,
    /// res = [(walltime, [("res_hierarchy", "properties_sql"), ...]), ...]
    pub res: Vec<(i64, Vec<(String, String)>)>,
    pub types: Vec<String>,
}

impl NewJob {
    pub fn insert(&self, session: &Session) -> Result<i64, Error> {
        session.runtime.block_on(async { self.insert_async(session).await })
    }
    /// Big unstructured piece of code since it should only be used by tests.
    async fn insert_async(&self, session: &Session) -> Result<i64, Error> {
        let launching_directory = "".to_string();
        let checkpoint_signal: i64 = 0;
        let properties = "".to_string();
        let queue_name = self.queue_name.clone();
        let job_user = self.user.clone().unwrap_or_else(|| "".to_string());

        let types: Vec<String> = self.types.clone();

        // Insert job
        let row = Query::insert()
            .into_table(Jobs::Table)
            .columns(vec![
                Alias::new(Jobs::LaunchingDirectory.to_string()),
                Alias::new(Jobs::CheckpointSignal.to_string()),
                Alias::new(Jobs::Properties.to_string()),
                Alias::new(Jobs::QueueName.to_string()),
                Alias::new(Jobs::User.to_string()),
            ])
            .values_panic(vec![
                Expr::val(&launching_directory),
                Expr::val(checkpoint_signal),
                Expr::val(&properties),
                Expr::val(&queue_name),
                Expr::val(&job_user),
            ])
            .returning_col(Jobs::Id)
            .fetch_one(session)
            .await?;
        let job_id: i64 = row.try_get(0)?;

        // For each moldable description
        let mut created_moldable_ids: Vec<i64> = Vec::new();
        for (walltime, groups) in self.res.iter() {
            // Insert moldable_job_descriptions
            let mld_row = Query::insert()
                .into_table(MoldableJobDescriptions::Table)
                .columns(vec![
                    Alias::new(MoldableJobDescriptions::JobId.to_string()),
                    Alias::new(MoldableJobDescriptions::Walltime.to_string()),
                ])
                .values_panic(vec![Expr::val(job_id), Expr::val(*walltime)])
                .returning_col(MoldableJobDescriptions::Id)
                .fetch_one(session)
                .await?;
            let moldable_id: i64 = mld_row.try_get(0)?;
            created_moldable_ids.push(moldable_id);

            // Insert job_resource_groups for each group
            for (res_hierarchy, prop_sql) in groups.iter() {
                let grp_row = Query::insert()
                    .into_table(JobResourceGroups::Table)
                    .columns(vec![
                        Alias::new(JobResourceGroups::MoldableId.to_string()),
                        Alias::new(JobResourceGroups::Property.to_string()),
                    ])
                    .values_panic(vec![Expr::val(moldable_id), Expr::val(prop_sql)])
                    .returning_col(JobResourceGroups::Id)
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
                            Alias::new(JobResourceDescriptions::GroupId.to_string()),
                            Alias::new(JobResourceDescriptions::ResourceType.to_string()),
                            Alias::new(JobResourceDescriptions::Value.to_string()),
                            Alias::new(JobResourceDescriptions::Order.to_string()),
                        ])
                        .values_panic(vec![
                            Expr::val(group_id),
                            Expr::val(k),
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
