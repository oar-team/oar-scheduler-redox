use crate::kao::slot::ProcSet;

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