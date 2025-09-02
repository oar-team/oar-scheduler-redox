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
    AssignedResourceIndex,
}
