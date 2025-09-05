/*
 * Copyright (c) 2025 Clément GRENNERAT
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the
 * GNU General Public License as published by the Free Software Foundation, version 3.
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU General Public License for more details.
 * You should have received a copy of the GNU General Public License along with this program.
 * If not, see https://www.gnu.org/licenses/.
 *
 */

use crate::platform::Platform;
use crate::test::setup_for_tests;
use oar_scheduler_db::model::queues::Queue;

#[test]
fn test_insert_and_get_queues() {
    let (session, config) = setup_for_tests(true);
    let mut platform = Platform::from_database(session, config);

    Queue {
        queue_name: "admin".to_string(),
        priority: 10,
        scheduler_policy: "kamelot".to_string(),
        state: "Active".to_string(),
    }
        .insert(&platform.session())
        .unwrap();

    Queue {
        queue_name: "default".to_string(),
        priority: 2,
        scheduler_policy: "kamelot".to_string(),
        state: "Active".to_string(),
    }
        .insert(&platform.session())
        .unwrap();

    Queue {
        queue_name: "besteffort".to_string(),
        priority: 0,
        scheduler_policy: "kamelot".to_string(),
        state: "Active".to_string(),
    }
        .insert(&platform.session())
        .unwrap();

    let queues = Queue::get_all_ordered_by_priority(&platform.session())
        .unwrap()
        .into_iter()
        .map(|q| q.queue_name)
        .collect::<Vec<String>>();
    assert_eq!(queues, vec!["admin".to_string(), "default".to_string(), "besteffort".to_string()]);

    Queue {
        queue_name: "besteffort2".to_string(),
        priority: 0,
        scheduler_policy: "kamelot".to_string(),
        state: "Active".to_string(),
    }
        .insert(&platform.session())
        .unwrap();

    Queue {
        queue_name: "admin2".to_string(),
        priority: 10,
        scheduler_policy: "kamelot".to_string(),
        state: "Active".to_string(),
    }
        .insert(&platform.session())
        .unwrap();

    Queue {
        queue_name: "default2".to_string(),
        priority: 2,
        scheduler_policy: "kamelot".to_string(),
        state: "Active".to_string(),
    }
        .insert(&platform.session())
        .unwrap();

    let queues = Queue::get_all_ordered_by_priority(&platform.session())
        .unwrap()
        .into_iter()
        .map(|q| q.queue_name)
        .collect::<Vec<String>>();
    assert_eq!(
        queues,
        vec![
            "admin".to_string(),
            "admin2".to_string(),
            "default".to_string(),
            "default2".to_string(),
            "besteffort".to_string(),
            "besteffort2".to_string(),
        ]
    );

    let grouped_queues = Queue::get_all_grouped_by_priority(&platform.session())
        .unwrap()
        .into_iter()
        .map(|qs| qs.into_iter().map(|q| q.queue_name).collect::<Vec<String>>())
        .collect::<Vec<Vec<String>>>();

    assert_eq!(grouped_queues.len(), 3);
    assert_eq!(grouped_queues[0].len(), 2);
    assert_eq!(grouped_queues[1].len(), 2);
    assert_eq!(grouped_queues[2].len(), 2);
    assert!(grouped_queues[0].contains(&"admin".to_string()));
    assert!(grouped_queues[0].contains(&"admin2".to_string()));
    assert!(grouped_queues[1].contains(&"default".to_string()));
    assert!(grouped_queues[1].contains(&"default2".to_string()));
    assert!(grouped_queues[2].contains(&"besteffort".to_string()));
    assert!(grouped_queues[2].contains(&"besteffort2".to_string()));
}
