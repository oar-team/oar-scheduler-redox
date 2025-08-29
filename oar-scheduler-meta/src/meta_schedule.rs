use crate::platform::Platform;
use crate::queues_schedule::queues_schedule;

pub async fn meta_schedule(platform: &mut Platform) {
    queues_schedule(platform).await;
}
