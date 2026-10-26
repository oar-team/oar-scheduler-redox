use crate::model::jobs::Jobs;
use crate::model::moldable::MoldableJobDescriptions;
use crate::{Session, SessionDeleteStatement, SessionInsertStatement};
use indexmap::IndexMap;
use log::debug;
use oar_scheduler_core::platform::Job;
use sea_query::{Expr, ExprTrait, Iden, Query};
use sqlx::Error;

#[derive(Iden)]
pub enum GanttJobsResources {
    #[iden = "gantt_jobs_resources"]
    Table,
    #[iden = "moldable_job_id"]
    MoldableId,
    #[iden = "resource_id"]
    ResourceId,
}

#[derive(Iden)]
pub enum GanttJobsPredictions {
    #[iden = "gantt_jobs_predictions"]
    Table,
    #[iden = "moldable_job_id"]
    MoldableId,
    #[iden = "start_time"]
    StartTime,
}

/// Removes all entries in the tables GanttJobsResources and GanttJobsPredictions except
/// the moldable ids of AR jobs with the reservation state `Scheduled` and the job state `Waiting` or `toAckReservation`.
pub fn gantt_flush_tables(session: &Session) {
    session.runtime.block_on(async {
        let to_keep_moldables_ids_req = Query::select()
            .column(MoldableJobDescriptions::Id)
            .from(MoldableJobDescriptions::Table)
            .inner_join(
                Jobs::Table,
                Expr::col((MoldableJobDescriptions::Table, MoldableJobDescriptions::JobId)).equals(Jobs::Id),
            )
            .inner_join(
                GanttJobsPredictions::Table,
                Expr::col((GanttJobsPredictions::Table, GanttJobsPredictions::MoldableId)).equals(MoldableJobDescriptions::Id),
            )
            .inner_join(
                GanttJobsResources::Table,
                Expr::col((GanttJobsResources::Table, GanttJobsResources::MoldableId)).equals(MoldableJobDescriptions::Id),
            )
            .and_where(Expr::col(Jobs::State).is_in(vec![
                Expr::value("Waiting").as_enum("job_state"),
                Expr::value("toAckReservation").as_enum("job_state"),
            ]))
            .and_where(Expr::col(Jobs::Reservation).eq("Scheduled"))
            .take();

        Query::delete()
            .from_table(GanttJobsResources::Table)
            .cond_where(Expr::col(GanttJobsResources::MoldableId).not_in_subquery(to_keep_moldables_ids_req.clone()))
            .execute(session)
            .await
            .expect("Failed to flush gantt_jobs_resources table");

        Query::delete()
            .from_table(GanttJobsPredictions::Table)
            .cond_where(Expr::col(GanttJobsPredictions::MoldableId).not_in_subquery(to_keep_moldables_ids_req.clone()))
            .execute(session)
            .await
            .expect("Failed to flush gantt_jobs_resources table");
    });
}

pub fn save_jobs_assignments_in_gantt(session: &Session, jobs: IndexMap<i64, Job>) -> Result<(), Error> {
    debug!("Saving {} assignments in gantt tables", jobs.len());
    if jobs.values().any(|job| job.assignment.is_none()) {
        panic!("Trying to save jobs assignments in gantt tables but some jobs have no assignment");
    }
    if jobs.is_empty() {
        debug!("No jobs to save in gantt tables");
        return Ok(());
    }
    session.runtime.block_on(async {
        let mut res_query = Query::insert()
            .into_table(GanttJobsResources::Table)
            .columns(vec![GanttJobsResources::MoldableId, GanttJobsResources::ResourceId])
            .take();
        let mut pred_query = Query::insert()
            .into_table(GanttJobsPredictions::Table)
            .columns(vec![GanttJobsPredictions::MoldableId, GanttJobsPredictions::StartTime])
            .take();

        for job in jobs.values() {
            let assignment = job.assignment.as_ref().unwrap();
            let moldable_id = &job.moldables[assignment.moldable_index].id;
            let begin = assignment.begin;

            pred_query.values_panic(vec![Expr::val(*moldable_id), Expr::val(begin)]);
            for res_id in &assignment.resources {
                res_query.values_panic(vec![Expr::val(*moldable_id), Expr::val(session.resource_index_to_resource_id(res_id))]);
            }
        }
        res_query.execute(session).await?;
        pred_query.execute(session).await?;
        Ok(())
    })
}
