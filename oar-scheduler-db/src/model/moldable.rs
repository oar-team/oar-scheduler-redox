use crate::{Session, SessionSelectStatement};
use oar_scheduler_core::model::job::Moldable;
use oar_scheduler_core::scheduler::hierarchy::HierarchyRequests;
use sea_query::{Expr, ExprTrait, Iden, Query};
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

pub struct AllJobMoldables {
    moldables: HashMap<i64, Vec<Moldable>>,
}
impl AllJobMoldables {
    pub(crate) async fn load_moldables_for_jobs(session: &Session, jobs: Vec<i64>) -> Result<Self, Error> {
        if jobs.is_empty() {
            return Ok(Self { moldables: HashMap::new() });
        }

        let moldables = Query::select()
            .columns(vec![
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
            .map(|row| {
                let job_id = row.get::<i64, &str>(MoldableJobDescriptions::JobId.to_string().as_str());

                // TODO: load moldable.

                (job_id, Moldable::new(1, 1, HierarchyRequests::from_requests(vec![])))
            })
            .fold(HashMap::new(), |mut acc, (job_id, moldable)| {
                acc.entry(job_id).or_insert_with(Vec::new).push(moldable);
                acc
            });

        Ok(Self { moldables })
    }
    pub fn get_job_moldables(&self, job_id: i64) -> Vec<Moldable> {
        self.moldables.get(&job_id).unwrap_or(&Vec::new()).clone()
    }
}
