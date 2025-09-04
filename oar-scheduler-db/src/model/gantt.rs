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

use crate::model::moldable::MoldableJobDescriptions;
use crate::model::Jobs;
use crate::{Session, SessionDeleteStatement};
use sea_query::{Expr, ExprTrait, Iden, Query};

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
            .inner_join(Jobs::Table, Expr::col(MoldableJobDescriptions::JobId).equals(Jobs::Id))
            .inner_join(
                Jobs::Table,
                Expr::col(GanttJobsPredictions::MoldableId).equals(MoldableJobDescriptions::Id),
            )
            .inner_join(Jobs::Table, Expr::col(GanttJobsResources::MoldableId).equals(MoldableJobDescriptions::Id))
            .and_where(Expr::col(Jobs::State).is_in(vec!["Waiting", "toAckReservation"]))
            .and_where(Expr::col(Jobs::Reservation).eq("Scheduled"))
            .take();

        Query::delete()
            .from_table(GanttJobsResources::Table)
            .cond_where(Expr::col(GanttJobsResources::MoldableId).in_subquery(to_keep_moldables_ids_req.clone()))
            .to_owned()
            .execute(session)
            .await
            .expect("Failed to flush gantt_jobs_resources table");

        Query::delete()
            .from_table(GanttJobsPredictions::Table)
            .cond_where(Expr::col(GanttJobsPredictions::MoldableId).in_subquery(to_keep_moldables_ids_req.clone()))
            .to_owned()
            .execute(session)
            .await
            .expect("Failed to flush gantt_jobs_resources table");
    });
}
