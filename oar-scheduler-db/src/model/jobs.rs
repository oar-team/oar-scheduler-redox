use sea_query::Iden;

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
