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

use sea_query::Iden;

#[derive(Iden)]
pub enum GanttJobsResources {
    #[iden = "gantt_jobs_resources"]
    Table,
    #[iden = "moldable_job_id"]
    MoldableJobId,
    #[iden = "resource_id"]
    ResourceId,
}

#[derive(Iden)]
pub enum GanttJobsPredictions {
    #[iden = "gantt_jobs_predictions"]
    Table,
    #[iden = "moldable_job_id"]
    MoldableJobId,
    #[iden = "start_time"]
    StartTime,
}


#[derive(Iden)]
pub enum AssignedResources {
    #[iden = "assigned_resources"]
    Table,
    #[iden = "moldable_job_id"]
    MoldableJobId,
    #[iden = "resource_id"]
    ResourceId,
    #[iden = "assigned_resource_index"]
    Index,
}
