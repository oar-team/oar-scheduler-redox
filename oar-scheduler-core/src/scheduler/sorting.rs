use crate::models::Job;
use crate::platform::PlatformTrait;
use indexmap::IndexMap;

pub fn sort_jobs<P>(platform: &P, waiting_jobs: &mut IndexMap<u32, Job>)
where
    P: PlatformTrait,
{}
