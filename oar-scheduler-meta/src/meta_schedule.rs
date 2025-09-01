use crate::platform::Platform;
use crate::queues_schedule::queues_schedule;

pub fn meta_schedule(platform: &mut Platform) {
    queues_schedule(platform);
}
