pub mod jobs;
pub mod events;
pub mod gantt;
pub mod resources;
pub mod admin;
pub mod queues;
pub mod job_types;
pub mod job_dependencies;
pub mod moldable;

pub trait SqlEnum {
    fn as_str(&self) -> &str;
    fn from_str(s: &str) -> Option<Self>
    where
        Self: Sized;
}
