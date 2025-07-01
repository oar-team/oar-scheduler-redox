
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
    pub submission_time: i32,
    pub start_time: i32,
    pub stop_time: i32,
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
    pub last_karma: Option<f32>
}