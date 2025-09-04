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

use crate::{Session, SessionInsertStatement, SessionSelectStatement};
use sea_query::{Expr, Iden, Query};
use sqlx::{Error, Row};
use std::collections::BTreeMap;

#[derive(Iden)]
enum Queues {
    #[iden = "queues"]
    Table,
    #[iden = "queue_name"]
    QueueName,
    #[iden = "priority"]
    Priority,
    #[iden = "scheduler_policy"]
    SchedulerPolicy,
    #[iden = "state"]
    State,
}

#[derive(Debug, Clone)]
pub struct Queue {
    pub queue_name: String,
    pub priority: i32,
    pub scheduler_policy: String,
    pub state: String,
}
impl Queue {
    pub fn insert(&self, session: &Session) -> Result<(), Error> {
        session.runtime.block_on(async {
            Query::insert()
                .into_table(Queues::Table)
                .columns(vec![Queues::QueueName, Queues::Priority, Queues::SchedulerPolicy, Queues::State])
                .values_panic(vec![
                    Expr::val(&self.queue_name),
                    Expr::val(self.priority),
                    Expr::val(&self.scheduler_policy),
                    Expr::val(&self.state),
                ])
                .execute(session)
                .await
        })?;
        Ok(())
    }

    /// Gets all queues ordered by priority (highest priority first).
    pub fn get_all_ordered_by_priority(session: &Session) -> Result<Vec<Queue>, Error> {
        let rows = session.runtime.block_on(async {
            Query::select()
                .columns(vec![Queues::QueueName, Queues::Priority, Queues::SchedulerPolicy, Queues::State])
                .from(Queues::Table)
                .order_by(Queues::Priority, sea_query::Order::Desc)
                .fetch_all(session)
                .await
        })?;

        let mut queues = Vec::new();
        for row in rows {
            let queue = Queue {
                queue_name: row.try_get("queue_name")?,
                priority: row.try_get("priority")?,
                scheduler_policy: row.try_get("scheduler_policy")?,
                state: row.try_get("state")?,
            };
            queues.push(queue);
        }
        Ok(queues)
    }

    /// Gets all queues grouped by priority (highest priority first).
    pub fn get_all_grouped_by_priority(session: &Session) -> Result<Vec<Vec<Queue>>, Error> {
        let rows = session.runtime.block_on(async {
            Query::select()
                .columns(vec![Queues::QueueName, Queues::Priority, Queues::SchedulerPolicy, Queues::State])
                .from(Queues::Table)
                .fetch_all(session)
                .await
        })?;

        // BTreeMap assures the ordering by increasing priority (key)
        let mut priority_map: BTreeMap<i32, Vec<Queue>> = BTreeMap::new();
        for row in rows {
            let queue = Queue {
                queue_name: row.try_get("queue_name")?,
                priority: row.try_get("priority")?,
                scheduler_policy: row.try_get("scheduler_policy")?,
                state: row.try_get("state")?,
            };
            priority_map.entry(queue.priority).or_default().push(queue);
        }

        Ok(priority_map.into_iter().map(|(_, v)| v).rev().collect::<Vec<Vec<Queue>>>())
    }
}
