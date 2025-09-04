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
use crate::model::moldable::{get_moldable_assigned_resources, AllJobMoldables, JobResourceDescriptions, JobResourceGroups, MoldableJobDescriptions};
use crate::{Session, SessionInsertStatement, SessionSelectStatement};
use indexmap::IndexMap;
use oar_scheduler_core::model::job::{JobAssignment, PlaceholderType, TimeSharingType};
use oar_scheduler_core::platform::Job;
use sea_query::{Alias, Expr, Query};
use sea_query::{ExprTrait, Iden};
use sqlx::{Error, Row};

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

/// Get jobs from database with various filters
/// If `queues` is `Some`, only jobs in these queues are returned
/// If `state` is `Some`, only jobs in this state are returned
/// Jobs are always ordered by start time and then by job id ascending, making the waiting jobs to be ordered by submission time, and scheduled/AR jobs by start time.
pub fn get_jobs(session: &Session, queues: Option<Vec<String>>, reservation: String, state: Option<String>) -> Result<IndexMap<i64, Job>, Error> {
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
                Jobs::AssignedMoldableJob,
            ])
            .from(Jobs::Table)
            .and_where(Expr::col(Jobs::Reservation).eq(reservation))
            .apply_if(queues, |req, queues| {
                req.and_where(Expr::col(Jobs::QueueName).is_in(queues));
            })
            .apply_if(state, |req, state| {
                req.and_where(Expr::col(Jobs::State).eq(state));
            })
            .order_by(Jobs::StartTime, sea_query::Order::Asc)
            .order_by(Jobs::Id, sea_query::Order::Asc)
            .to_owned()
            .fetch_all(session)
            .await?;

        let job_ids = rows
            .iter()
            .map(|r| r.get::<i64, &str>(Jobs::Id.to_string().as_str()))
            .collect::<Vec<i64>>();

        let jobs_types = AllJobTypes::load_type_for_jobs(session, job_ids.clone()).await?;
        let jobs_dependencies = AllJobDependencies::load_dependencies_for_jobs(session, job_ids.clone()).await?;
        let jobs_moldables = AllJobMoldables::load_moldables_for_jobs(session, job_ids).await?;

        let mut jobs = IndexMap::new();
        for row in rows {
            let id: i64 = row.get(Jobs::Id.to_string().as_str());
            let types = jobs_types.get_job_types(id);
            let moldables = jobs_moldables.get_job_moldables(id);

            // Reservation jobs
            let reservation: String = row.get(Jobs::Reservation.to_string().as_str());
            let advance_reservation_start_time = if reservation == "toSchedule" {
                Some(row.get::<i64, &str>(Jobs::StartTime.to_string().as_str()))
            } else {
                None
            };

            // Assigned jobs
            let mut assignment = None;
            let assigned_moldable_job: Option<i64> = row.try_get(Jobs::AssignedMoldableJob.to_string().as_str()).ok();
            if let Some(assigned_id) = assigned_moldable_job {
                if assigned_id != 0 {
                    if let Some(index) = moldables.iter().position(|m| m.id == assigned_id) {
                        assignment = Some(JobAssignment {
                            begin: row.get::<i64, &str>(Jobs::StartTime.to_string().as_str()),
                            end: row.get::<i64, &str>(Jobs::StopTime.to_string().as_str()),
                            proc_set: get_moldable_assigned_resources(session, assigned_id).await?,
                            moldable_index: index,
                        })
                    }
                }
            }

            let job = Job {
                id,
                name: row.try_get(Jobs::Name.to_string().as_str()).map(|s: String| s.into_boxed_str()).ok(),
                user: row.try_get(Jobs::User.to_string().as_str()).map(|s: String| s.into_boxed_str()).ok(),
                project: row.try_get(Jobs::Project.to_string().as_str()).map(|s: String| s.into_boxed_str()).ok(),
                queue: row.get::<String, &str>(Jobs::QueueName.to_string().as_str()).into_boxed_str(),
                moldables,
                no_quotas: types.contains_key("no_quotas"),
                assignment,
                quotas_hit_count: 0,
                time_sharing: TimeSharingType::from_types(&types),
                placeholder: PlaceholderType::from_types(&types),
                dependencies: jobs_dependencies.get_job_dependencies(id),
                advance_reservation_start_time,
                submission_time: row.get::<i64, &str>(Jobs::SubmissionTime.to_string().as_str()),
                types,
                qos: 0.0,
                nice: 0.0,
                karma: 0.0,
            };
            jobs.insert(id, job);
        }
        Ok::<IndexMap<i64, Job>, Error>(jobs)
    })?;
    Ok(jobs)
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
