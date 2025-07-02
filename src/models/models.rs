use crate::scheduler::slot::ProcSet;
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub enum JobTypeEnum {
    Interactive,
    Passive
}

#[derive(Debug, Clone)]
pub enum JobStateEnum {
    Waiting, // the job is waiting OAR scheduler decision.
    Hold, // user or administrator wants to hold the job (oarhold command). So it will not be scheduled by the system.
    toLaunch, // the OAR scheduler has attributed some nodes to the job. So it will be launched.
    toError, // something wrong occurred and the job is going into the error state.
    toAckReservation, // the OAR scheduler must say “YES” or “NO” to the waiting oarsub command because it requested a reservation.
    Launching, //  OAR has launched the job and will execute the user command on the first node.
    Running, // the user command is executing on the first node.
    Suspended, // the job was in Running state and there was a request (oarhold with “-r” option) to suspend this job. In this state other jobs can be scheduled on the same resources (these resources has the “suspended_jobs” field to “YES”).
    Resuming,
    Finishing, // the user command has terminated and OAR is doing work internally
    Terminated, // the job has terminated normally
    Error, // a problem has occurred
}

#[derive(Debug, Clone)]
pub enum JobReservationEnum {
    None, // the job is not a reservation.
    toSchedule, // the job is a reservation and must be approved by the scheduler.
    Scheduled // the job is a reservation and is scheduled by OAR.
}

#[derive(Debug, Clone)]
pub enum JobSuspendedEnum {
    Yes,
    No
}

#[derive(Debug, Clone)]
pub enum JobAccountedEnum {
    Yes,
    No
}

// Job model with moldable and resource information
#[derive(Debug, Clone)]
pub struct Job {
    pub id: u32, // job identifier
    pub array_id: i32, // array identifier
    pub array_index: i32, // index of the job in the array
    pub initial_request: Option<String>, // oarsub initial arguments
    pub name: Option<String>, // name given by the user
    pub env: Option<String>, // name of the cpuset directory used for this job on each nodes
    pub type_: JobTypeEnum, // specify if the user wants to launch a program or get an interactive shell
    pub info_type: Option<String>, // some informations about oarsub command
    pub state: JobStateEnum, // job state
    pub reservation: JobReservationEnum, // specify if the job is a reservation and the state of this one
    pub message: String, // readable information message for the user
    pub user: String, // user name
    pub command: Option<String>, // program to run
    pub queue_name: String,
    pub properties: Option<String>, // properties that assigned nodes must match
    pub launching_directory: String, // path of the directory where to launch the user process
    pub submission_time: i64,
    pub start_time: i64,
    pub stop_time: i64,
    pub file_id: Option<i32>,
    pub accounted: JobAccountedEnum, // specify if the job was considered by the accounting mechanism or not
    pub notify: Option<String>, // gives the way to notify the user about the job (mail or script )
    pub assigned_moldable_job: Option<i32>, // 
    pub checkpoint: i32,
    pub checkpoint_signal: i32,
    pub stdout_file: Option<String>,
    pub stderr_file: Option<String>,
    pub resubmit_job_id: i32,
    pub project: String,
    pub suspended: JobSuspendedEnum,
    pub exit_code: Option<i32>,
    pub group: String,

    pub scheduler_info: String,
    pub last_karma: Option<f32>,

    // Moldable
    pub walltime: i64,

    // Other
    pub res_set: ProcSet,
}

#[derive(Debug, Clone)]
pub struct Accounting {
    pub window_start: i64,
    pub window_stop: i64,
    pub user: String,
    pub project: String,
    pub queue_name: String,
    pub consumption_type: String,
    pub consumption: i64,
}

#[derive(Debug, Clone)]
pub struct AdmissionRule {
    pub id: i32,
    pub rule: String,
    pub priority: i32,
    pub enabled: String,
}

#[derive(Debug, Clone)]
pub struct AssignedResource {
    pub moldable_id: i32,
    pub resource_id: i32,
    pub index: String,
}

#[derive(Debug, Clone)]
pub struct Challenge {
    pub job_id: i32,
    pub challenge: String,
    pub ssh_private_key: String,
    pub ssh_public_key: String,
}

#[derive(Debug, Clone)]
pub struct EventLogHostname {
    pub event_id: i32,
    pub hostname: String,
}

#[derive(Debug, Clone)]
pub struct EventLog {
    pub id: i32,
    pub type_: String,
    pub job_id: i32,
    pub date: i32,
    pub description: String,
    pub to_check: String,
}

#[derive(Debug, Clone)]
pub struct File {
    pub id: i32,
    pub md5sum: Option<String>,
    pub location: Option<String>,
    pub method: Option<String>,
    pub compression: Option<String>,
    pub size: i32,
}

#[derive(Debug, Clone)]
pub struct FragJob {
    pub job_id: i32,
    pub date: i32,
    pub state: String,
}

#[derive(Debug, Clone)]
pub struct GanttJobsPrediction {
    pub moldable_id: i32,
    pub start_time: i32,
}

#[derive(Debug, Clone)]
pub struct GanttJobsPredictionsLog {
    pub sched_date: i32,
    pub moldable_id: i32,
    pub start_time: i32,
}

#[derive(Debug, Clone)]
pub struct GanttJobsPredictionsVisu {
    pub moldable_id: i32,
    pub start_time: i32,
}

#[derive(Debug, Clone)]
pub struct GanttJobsResource {
    pub moldable_id: i32,
    pub resource_id: i32,
}

#[derive(Debug, Clone)]
pub struct GanttJobsResourcesLog {
    pub sched_date: i32,
    pub moldable_id: i32,
    pub resource_id: i32,
}

#[derive(Debug, Clone)]
pub struct GanttJobsResourcesVisu {
    pub moldable_id: i32,
    pub resource_id: i32,
}

#[derive(Debug, Clone)]
pub struct JobDependency {
    pub job_id: i32,
    pub job_id_required: i32,
    pub index: String,
}

#[derive(Debug, Clone)]
pub struct JobResourceDescription {
    pub group_id: i32,
    pub resource_type: String,
    pub value: i32,
    pub order: i32,
    pub index: String,
}

#[derive(Debug, Clone)]
pub struct JobResourceGroup {
    pub id: i32,
    pub moldable_id: i32,
    pub property: Option<String>,
    pub index: String,
}

#[derive(Debug, Clone)]
pub struct JobStateLog {
    pub id: i32,
    pub job_id: i32,
    pub job_state: String,
    pub date_start: i32,
    pub date_stop: i32,
}

#[derive(Debug, Clone)]
pub struct JobType {
    pub id: i32,
    pub job_id: i32,
    pub type_: String,
    pub types_index: String,
}

#[derive(Debug, Clone)]
pub struct Resource {
    pub id: i32,
    pub type_: String,
    pub network_address: String,
    pub state: String,
    pub next_state: String,
    pub finaud_decision: String,
    pub next_finaud_decision: String,
    pub state_num: i32,
    pub suspended_jobs: String,
    pub scheduler_priority: i64,
    pub cpuset: String,
    pub besteffort: String,
    pub deploy: String,
    pub expiry_date: i32,
    pub desktop_computing: String,
    pub last_job_date: i32,
    pub available_upto: i32,
    pub last_available_upto: i32,
    pub drain: String,
}

#[derive(Debug, Clone)]
pub struct MoldableJobDescription {
    pub id: i32,
    pub job_id: i32,
    pub walltime: i32,
    pub index: String,
}

#[derive(Debug, Clone)]
pub struct Queue {
    pub name: String,
    pub priority: i32,
    pub scheduler_policy: String,
    pub state: String,
}

#[derive(Debug, Clone)]
pub struct ResourceLog {
    pub id: i32,
    pub resource_id: i32,
    pub attribute: String,
    pub value: String,
    pub date_start: i32,
    pub date_stop: i32,
    pub finaud_decision: String,
}

#[derive(Debug, Clone)]
pub struct Scheduler {
    pub name: String,
    pub script: String,
    pub description: String,
}

#[derive(Debug, Clone)]
pub struct WalltimeChange {
    pub job_id: i32,
    pub pending: i32,
    pub force: String,
    pub delay_next_jobs: String,
    pub granted: i32,
    pub granted_with_force: i32,
    pub granted_with_delay_next_jobs: i32,
}

impl Job {
    pub fn new_waiting(id: u32, walltime: i64, res_set: ProcSet) -> Job {
        Self::new(id, 0, 0, walltime, JobStateEnum::Waiting, JobReservationEnum::None, res_set)
    }
    pub fn new_reserved(id: u32, start_time: i64, end_time: i64, walltime: i64, res_set: ProcSet) -> Job {
        Self::new(id, start_time, end_time, walltime, JobStateEnum::toLaunch, JobReservationEnum::Scheduled, res_set)
    }
    pub fn new_scheduled(id: u32, start_time: i64, end_time: i64, walltime: i64, res_set: ProcSet) -> Job {
        Self::new(id, start_time, end_time, walltime, JobStateEnum::toLaunch, JobReservationEnum::None, res_set)
    }
    pub fn new(id: u32, start_time: i64, end_time: i64, walltime: i64, state: JobStateEnum, reservation: JobReservationEnum, res_set: ProcSet) -> Job {
        Job {
            id,
            array_id: 0,
            array_index: 0,
            initial_request: None,
            name: None,
            env: None,
            type_: JobTypeEnum::Passive,
            info_type: None,
            state: JobStateEnum::toLaunch,
            reservation: JobReservationEnum::toSchedule,
            message: String::new(),
            scheduler_info: String::new(),
            user: String::new(),
            project: String::new(),
            group: String::new(),
            command: None,
            exit_code: None,
            queue_name: String::new(),
            properties: None,
            launching_directory: String::new(),
            submission_time: 0,
            start_time,
            stop_time: 0,
            file_id: None,
            accounted: JobAccountedEnum::No,
            notify: None,
            assigned_moldable_job: None,
            checkpoint: 0,
            checkpoint_signal: 0,
            stdout_file: None,
            stderr_file: None,
            resubmit_job_id: 0,
            suspended: JobSuspendedEnum::No,
            last_karma: None,
            walltime,
            res_set,
        }
    }
}
