use crate::kao::slot::ProcSet;
use std::collections::HashMap;

// Job model with moldable and resource information
#[derive(Debug, Clone)]
pub struct Job {
    pub id: i32,
    pub array_id: i32,
    pub array_index: i32,
    pub initial_request: Option<String>,
    pub name: Option<String>,
    pub env: Option<String>,
    pub type_: String, // Enum ?
    pub info_type: Option<String>,
    pub state: String,
    pub reservation: String,
    pub message: String,
    pub scheduler_info: String,
    pub user: String,
    pub project: String,
    pub group: String,
    pub command: Option<String>,
    pub exit_code: Option<i32>,
    pub queue_name: String,
    pub properties: Option<String>,
    pub launching_directory: String,
    pub submission_time: i64,
    pub start_time: i64,
    pub stop_time: i64,
    pub file_id: Option<i32>,
    pub accounted: String, // Enum ?
    pub notify: Option<String>,
    pub assigned_moldable_job: Option<i32>,
    pub checkpoint: i32,
    pub checkpoint_signal: i32,
    pub stdout_file: Option<String>,
    pub stderr_file: Option<String>,
    pub resubmit_job_id: i32,
    pub suspended: String, // Enum ?
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
    pub fn new(id: i32, start_time: i64, walltime: i64, res_set: ProcSet) -> Job {
        Job {
            id,
            array_id: 0,
            array_index: 0,
            initial_request: None,
            name: None,
            env: None,
            type_: String::new(),
            info_type: None,
            state: String::new(),
            reservation: String::new(),
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
            accounted: String::new(),
            notify: None,
            assigned_moldable_job: None,
            checkpoint: 0,
            checkpoint_signal: 0,
            stdout_file: None,
            stderr_file: None,
            resubmit_job_id: 0,
            suspended: String::new(),
            last_karma: None,
            walltime,
            res_set,
        }
    }
}