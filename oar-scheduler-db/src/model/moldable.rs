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

use crate::model::gantt::{GanttJobsPredictions, GanttJobsResources};
use crate::model::jobs::Jobs;
use crate::{Session, SessionSelectStatement};
use oar_scheduler_core::model::job::ProcSet;
use oar_scheduler_core::model::job::{JobAssignment, Moldable};
use oar_scheduler_core::scheduler::hierarchy::{HierarchyRequest, HierarchyRequests};
use sea_query::{Expr, ExprTrait, Iden, Query};
use sqlx::any::AnyRow;
use sqlx::{Error, Row};
use std::collections::HashMap;

#[derive(Iden)]
pub enum MoldableJobDescriptions {
    #[iden = "moldable_job_descriptions"]
    Table,
    #[iden = "moldable_id"]
    Id,
    #[iden = "moldable_job_id"]
    JobId,
    #[iden = "moldable_walltime"]
    Walltime,
    #[iden = "moldable_index"]
    Index,
}
#[derive(Iden)]
pub enum JobResourceDescriptions {
    #[iden = "job_resource_descriptions"]
    Table,
    #[iden = "res_job_group_id"]
    GroupId,
    #[iden = "res_job_resource_type"]
    ResourceType,
    #[iden = "res_job_value"]
    Value,
    #[iden = "res_job_order"]
    Order,
    #[iden = "res_job_index"]
    Index,
}
#[derive(Iden)]
pub enum JobResourceGroups {
    #[iden = "job_resource_groups"]
    Table,
    #[iden = "res_group_id"]
    Id,
    #[iden = "res_group_moldable_id"]
    MoldableId,
    #[iden = "res_group_property"]
    Property,
    #[iden = "res_group_index"]
    Index,
}
#[derive(Iden)]
pub enum AssignedResources {
    #[iden = "assigned_resources"]
    Table,
    #[iden = "moldable_job_id"]
    MoldableId,
    #[iden = "resource_id"]
    ResourceId,
    #[iden = "assigned_resource_index"]
    Index,
}

pub struct AllJobMoldables {
    moldables: HashMap<i64, Vec<Moldable>>,
}
impl AllJobMoldables {
    pub(crate) async fn load_moldables_for_jobs(session: &Session, jobs: Vec<i64>) -> Result<Self, Error> {
        if jobs.is_empty() {
            return Ok(Self { moldables: HashMap::new() });
        }
        // Sleep 1s
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        let moldables = Query::select()
            .columns(vec![
                MoldableJobDescriptions::Id.to_string(),
                MoldableJobDescriptions::JobId.to_string(),
                MoldableJobDescriptions::Walltime.to_string(),
                MoldableJobDescriptions::Index.to_string(),
                JobResourceGroups::Id.to_string(),
                JobResourceGroups::Property.to_string(),
                JobResourceDescriptions::GroupId.to_string(),
                JobResourceDescriptions::ResourceType.to_string(),
                JobResourceDescriptions::Value.to_string(),
            ])
            .from(MoldableJobDescriptions::Table)
            .left_join(
                JobResourceGroups::Table,
                Expr::col(MoldableJobDescriptions::Id).equals(JobResourceGroups::MoldableId),
            )
            .left_join(
                JobResourceDescriptions::Table,
                Expr::col(JobResourceGroups::Id).equals(JobResourceDescriptions::GroupId),
            )
            .and_where(Expr::col(MoldableJobDescriptions::JobId).is_in(jobs))
            .and_where(Expr::col(MoldableJobDescriptions::Index).eq("CURRENT"))
            .and_where(Expr::col(JobResourceGroups::Index).eq("CURRENT"))
            .and_where(Expr::col(JobResourceDescriptions::Index).eq("CURRENT"))
            .order_by(MoldableJobDescriptions::Id, sea_query::Order::Asc)
            .order_by(JobResourceGroups::Id, sea_query::Order::Asc)
            .order_by(JobResourceDescriptions::Order, sea_query::Order::Asc)
            .fetch_all(session)
            .await?
            .iter()
            .fold(
                // job_id -> moldable_id -> (walltime, group_id -> level_nbs)
                HashMap::<i64, HashMap<i64, (i64, HashMap<i64, Vec<(Box<str>, u32)>>)>>::new(),
                |mut acc, row| {
                    let job_id: i64 = row.get(MoldableJobDescriptions::JobId.unquoted());
                    let mld_id: i64 = row.get(MoldableJobDescriptions::Id.unquoted());
                    let walltime: i64 = row.get(MoldableJobDescriptions::Walltime.unquoted());
                    let group_id: i64 = row.get(JobResourceGroups::Id.unquoted());
                    let rtype: String = row.get(JobResourceDescriptions::ResourceType.unquoted());
                    let rvalue: i64 = row.get(JobResourceDescriptions::Value.unquoted());

                    acc.entry(job_id)
                        .or_insert_with(HashMap::new)
                        .entry(mld_id)
                        .or_insert_with(|| (walltime, HashMap::<i64, Vec<(Box<str>, u32)>>::new()))
                        .1
                        .entry(group_id)
                        .or_insert_with(Vec::new)
                        .push((rtype.into_boxed_str(), rvalue as u32));
                    acc
                },
            )
            .into_iter()
            .map(|(job_id, mlds)| {
                let molds = mlds
                    .into_iter()
                    .map(|(mld_id, (walltime, groups_map))| {
                        // Build one HierarchyRequest per resource group
                        let mut group_ids: Vec<i64> = groups_map.keys().cloned().collect();
                        group_ids.sort_unstable();
                        let reqs: Vec<HierarchyRequest> = group_ids
                            .into_iter()
                            .filter_map(|gid| groups_map.get(&gid).cloned())
                            .map(|levels| HierarchyRequest::new(!ProcSet::new(), levels))
                            .collect();
                        Moldable::new(mld_id, walltime, HierarchyRequests::from_requests(reqs))
                    })
                    .collect::<Vec<Moldable>>();
                (job_id, molds)
            })
            .collect::<HashMap<i64, Vec<Moldable>>>();

        Ok(Self { moldables })
    }

    pub fn get_job_moldables(&self, job_id: i64) -> Vec<Moldable> {
        self.moldables.get(&job_id).unwrap_or(&Vec::new()).clone()
    }

    /// Get the moldable assignment for a job.
    /// If `properties_from_gantt` is true, the resources are fetched from the gantt table `gantt_jobs_resources`,
    /// and the start time from the `gantt_jobs_prediction` table.
    /// Otherwise, they are fetched from the table `assigned_resources` and the job `start_time` column.
    /// The `job_row` parameter is the row of the job in the jobs table. It should contain at least the columns `Jobs::Id`, `Jobs::AssignedMoldableJob`, and:
    /// - if `properties_from_gantt` is false, `Jobs::StartTime` and `Jobs::StopTime`.
    /// - if `properties_from_gantt` is true, `GanttJobsPredictions::StartTime` (in this case the end time is computed from the start time and the moldable walltime).
    pub(crate) async fn get_job_assignment(&self, session: &Session, job_row: &AnyRow, properties_from_gantt: bool) -> Option<JobAssignment> {
        let job_id: i64 = job_row.get(Jobs::Id.unquoted());
        let assigned_moldable_id: i64 = job_row.get(Jobs::AssignedMoldableId.unquoted());
        if assigned_moldable_id == 0 {
            return None;
        }
        let job_moldables = self.get_job_moldables(job_id);
        let moldable_index = job_moldables.iter().position(|m| m.id == assigned_moldable_id)?;
        let moldable = &job_moldables[moldable_index];

        // Get assigned resources
        let resources = if properties_from_gantt {
            Query::select()
                .columns(vec![GanttJobsResources::ResourceId])
                .from(GanttJobsResources::Table)
                .and_where(Expr::col(GanttJobsResources::MoldableId).eq(moldable.id))
                .fetch_all(session)
                .await
                .unwrap()
        } else {
            Query::select()
                .columns(vec![AssignedResources::ResourceId])
                .from(AssignedResources::Table)
                .and_where(Expr::col(AssignedResources::MoldableId).eq(moldable.id))
                .and_where(Expr::col(AssignedResources::Index).eq("CURRENT"))
                .fetch_all(session)
                .await
                .unwrap()
        };
        let resources: ProcSet = ProcSet::from_iter(resources.iter().map(|row| {
            let res_id: i32 = row.get(AssignedResources::ResourceId.unquoted());
            session
                .resource_id_to_resource_index(res_id)
                .expect("Resource not found. There might be a database concurrency issue.")
        }));

        // Get assigned start time
        let (begin, end) = if properties_from_gantt {
            let start_time: i64 = job_row.get(GanttJobsPredictions::StartTime.unquoted());
            let stop_time = start_time + moldable.walltime - 1;
            (start_time, stop_time)
        } else {
            let start_time: i64 = job_row.get(Jobs::StartTime.unquoted());
            let stop_time: i64 = job_row.get(Jobs::StopTime.unquoted());
            (start_time, stop_time)
        };

        Some(JobAssignment {
            begin,
            end,
            resources,
            moldable_index,
        })
    }
}
