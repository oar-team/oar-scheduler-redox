use crate::platform::Platform;
use crate::test::setup_for_tests;
use oar_scheduler_core::model::configuration::Configuration;
use oar_scheduler_core::platform::{PlatformTrait, ProcSet};
use oar_scheduler_core::scheduler::hierarchy::{HierarchyRequest, HierarchyRequests};
use oar_scheduler_db::model::{NewResource, NewResourceColumn, ResourceLabelValue};
use oar_scheduler_db::Session;

fn create_resources_hierarchy(session: &Session, config: &mut Configuration) {
    NewResourceColumn {
        name: "core".to_string(),
        r#type: "Integer".to_string(),
    }
        .insert(session)
        .expect("Failed to insert test resource column");
    NewResourceColumn {
        name: "cpu".to_string(),
        r#type: "Integer".to_string(),
    }
        .insert(session)
        .expect("Failed to insert test resource column");
    NewResourceColumn {
        name: "host".to_string(),
        r#type: "Varchar(255)".to_string(),
    }
        .insert(session)
        .expect("Failed to insert test resource column");
    NewResourceColumn {
        name: "mem".to_string(),
        r#type: "Integer".to_string(),
    }
        .insert(session)
        .expect("Failed to insert test resource column");

    config.hierarchy_labels = Some("resource_id,network_address,core,cpu,host,mem".to_string());
}

#[test]
fn create_resources_test() {
    let (session, mut config) = setup_for_tests();

    create_resources_hierarchy(&session, &mut config);

    NewResource {
        network_address: "100.64.0.1".to_string(),
        r#type: "default".to_string(),
        state: "alive".to_string(),
        labels: indexmap::indexmap! {
            "core".to_string() => ResourceLabelValue::Integer(1),
            "cpu".to_string() => ResourceLabelValue::Integer(1),
            "host".to_string() => ResourceLabelValue::Varchar("node1".to_string()),
            "mem".to_string() => ResourceLabelValue::Integer(1),
        },
    }
        .insert(&session)
        .expect("Failed to insert test resource");

    NewResource {
        network_address: "100.64.0.1".to_string(),
        r#type: "default".to_string(),
        state: "alive".to_string(),
        labels: indexmap::indexmap! {
            "core".to_string() => ResourceLabelValue::Integer(2),
            "cpu".to_string() => ResourceLabelValue::Integer(1),
            "host".to_string() => ResourceLabelValue::Varchar("node1".to_string()),
            "mem".to_string() => ResourceLabelValue::Integer(1),
        },
    }
        .insert(&session)
        .expect("Failed to insert test resource");

    NewResource {
        network_address: "100.64.0.2".to_string(),
        r#type: "default".to_string(),
        state: "alive".to_string(),
        labels: indexmap::indexmap! {
            "core".to_string() => ResourceLabelValue::Integer(3),
            "cpu".to_string() => ResourceLabelValue::Integer(2),
            "host".to_string() => ResourceLabelValue::Varchar("node2".to_string()),
            "mem".to_string() => ResourceLabelValue::Integer(2),
        },
    }
        .insert(&session)
        .expect("Failed to insert test resource");

    let platform = Platform::from_database(session, config);
    let resource_set = &platform.get_platform_config().resource_set;
    assert_eq!(resource_set.default_resources, ProcSet::from_iter(0..=2));

    if resource_set.hierarchy.unit_partitions() != &vec![Box::from("core"), Box::from("resource_id")]
        && resource_set.hierarchy.unit_partitions() != &vec![Box::from("resource_id"), Box::from("core")]
    {
        panic!("Unexpected unit_partitions: {:?}", resource_set.hierarchy.unit_partitions());
    }

    let request = HierarchyRequests::from_requests(vec![HierarchyRequest::new(
        resource_set.default_resources.clone(),
        vec![(Box::from("resource_id"), 2)],
    )]);
    assert_eq!(
        resource_set.hierarchy.request(&resource_set.default_resources, &request),
        Some(ProcSet::from_iter(0..=1))
    );

    let request = HierarchyRequests::from_requests(vec![HierarchyRequest::new(
        resource_set.default_resources.clone(),
        vec![(Box::from("core"), 3)],
    )]);
    assert_eq!(
        resource_set.hierarchy.request(&resource_set.default_resources, &request),
        Some(ProcSet::from_iter(0..=2))
    );

    let request = HierarchyRequests::from_requests(vec![HierarchyRequest::new(
        resource_set.default_resources.clone(),
        vec![(Box::from("cpu"), 2)],
    )]);
    assert_eq!(
        resource_set.hierarchy.request(&resource_set.default_resources, &request),
        Some(ProcSet::from_iter(0..=2))
    );

    let request = HierarchyRequests::from_requests(vec![HierarchyRequest::new(
        resource_set.default_resources.clone(),
        vec![(Box::from("mem"), 1)],
    )]);
    if resource_set.hierarchy.request(&resource_set.default_resources, &request) != Some(ProcSet::from_iter(2..=2))
        && resource_set.hierarchy.request(&resource_set.default_resources, &request) != Some(ProcSet::from_iter(0..=1))
    {
        panic!(
            "Unexpected request resultX: {:?}",
            resource_set.hierarchy.request(&resource_set.default_resources, &request)
        );
    }
}
