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
pub enum MoldableJobDescriptions {
    #[iden = "moldable_job_descriptions"]
    Table,
    #[iden = "moldable_id"]
    MoldableId,
    #[iden = "moldable_job_id"]
    MoldableJobId,
    #[iden = "moldable_walltime"]
    MoldableWalltime,
    #[iden = "moldable_index"]
    MoldableIndex,
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

#[derive(Iden)]
pub enum JobResourceDescriptions {
    #[iden = "job_resource_descriptions"]
    Table,
    #[iden = "res_job_group_id"]
    ResJobGroupId,
    #[iden = "res_job_resource_type"]
    ResJobResourceType,
    #[iden = "res_job_value"]
    ResJobValue,
    #[iden = "res_job_order"]
    ResJobOrder,
    #[iden = "res_job_index"]
    ResJobIndex,
}

#[derive(Iden)]
pub enum JobResourceGroups {
    #[iden = "job_resource_groups"]
    Table,
    #[iden = "res_group_id"]
    ResGroupId,
    #[iden = "res_group_moldable_id"]
    ResGroupMoldableId,
    #[iden = "res_group_property"]
    ResGroupProperty,
    #[iden = "res_group_index"]
    ResGroupIndex,
}
