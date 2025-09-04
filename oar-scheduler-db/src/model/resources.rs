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
use indexmap::IndexMap;
use sea_query::{Alias, Expr, Iden, Query};
use sqlx::{Error, Row};
use std::collections::HashMap;

#[derive(Iden)]
enum Resources {
    #[iden = "resources"]
    Table,
    #[iden = "resource_id"]
    ResourceId,
    #[iden = "network_address"]
    NetworkAddress,
    #[iden = "type"]
    Type,
    #[iden = "state"]
    State,
    #[iden = "next_state"]
    NextState,
    #[iden = "state_num"]
    StateNum,
    #[iden = "finaud_decision"]
    FinaudDecision,
    #[iden = "next_finaud_decision"]
    NextFinaudDecision,
    #[iden = "suspended_jobs"]
    SuspendedJobs,
    #[iden = "scheduler_priority"]
    SchedulerPriority,
    #[iden = "cpuset"]
    CpuSet,
    #[iden = "available_upto"]
    AvailableUpto,
    #[iden = "besteffort"]
    BestEffort,
    #[iden = "drain"]
    Drain,
}

#[derive(Iden)]
enum ResourceLogs {
    #[iden = "resource_logs"]
    Table,
    #[iden = "resource_log_id"]
    ResourceLogId,
    #[iden = "resource_id"]
    ResourceId,
    #[iden = "attribute"]
    Attribute,
    #[iden = "value"]
    Value,
    #[iden = "date_start"]
    DateStart,
    #[iden = "date_stop"]
    DateStop,
    #[iden = "finaud_decision"]
    FinaudDecision,
}

#[derive(Iden)]
enum Files {
    #[iden = "files"]
    Table,
    #[iden = "file_id"]
    FileId,
    #[iden = "md5sum"]
    Md5sum,
    #[iden = "location"]
    Location,
    #[iden = "method"]
    Method,
    #[iden = "compression"]
    Compression,
    #[iden = "size"]
    Size,
}



#[derive(Iden)]
enum Accounting {
    #[iden = "accounting"]
    Table,
    #[iden = "window_start"]
    WindowStart,
    #[iden = "window_stop"]
    WindowStop,
    #[iden = "accounting_project"]
    AccountingProject,
    #[iden = "accounting_user"]
    AccountingUser,
    #[iden = "queue_name"]
    QueueName,
    #[iden = "consumption_type"]
    ConsumptionType,
    #[iden = "consumption"]
    Consumption,
}

pub struct NewResource {
    pub network_address: String,
    pub r#type: String,
    pub state: String,
    pub labels: IndexMap<String, ResourceLabelValue>,
}
impl NewResource {
    pub fn insert(&self, session: &Session) -> Result<i64, Error> {
        let columns = vec![
            Alias::new(Resources::NetworkAddress.to_string()),
            Alias::new(Resources::Type.to_string()),
            Alias::new(Resources::State.to_string()),
        ]
            .into_iter()
            .chain(self.labels.keys().map(|k| Alias::new(k)))
            .collect::<Vec<Alias>>();
        let values = vec![Expr::val(&self.network_address), Expr::val(&self.r#type), Expr::val(&self.state)]
            .into_iter()
            .chain(self.labels.values().map(|v| match v {
                ResourceLabelValue::Integer(i) => Expr::val(*i),
                ResourceLabelValue::Varchar(s) => Expr::val(s),
            }))
            .collect::<Vec<Expr>>();

        let row = session.runtime.block_on(async {
            Query::insert()
                .into_table(Resources::Table)
                .columns(columns)
                .values_panic(values)
                .returning_col(Resources::ResourceId)
                .fetch_one(session)
                .await
        })?;
        Ok(row.try_get::<i64, _>(0)?)
    }
}

pub struct NewResourceColumn {
    pub name: String,
    pub r#type: String,
}
impl NewResourceColumn {
    pub fn insert(&self, session: &Session) -> Result<(), Error> {
        session.runtime.block_on(async {
            match session.backend {
                crate::Backend::Postgres => {
                    let sql = format!("ALTER TABLE resources ADD COLUMN {} {};", self.name, self.r#type);
                    sqlx::query(&sql).execute(&session.pool).await?;
                }
                crate::Backend::Sqlite => {
                    let sql = format!("ALTER TABLE resources ADD COLUMN {} {};", self.name, self.r#type);
                    sqlx::query(&sql).execute(&session.pool).await?;
                }
            }
            Ok(())
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ResourceLabelValue {
    Integer(i64),
    Varchar(String),
}

pub struct Resource {
    pub id: i32,
    pub r#type: String,
    pub state: String,
    pub available_upto: Option<i64>,
    pub labels: HashMap<Box<str>, ResourceLabelValue>,
}
impl Resource {
    /// Get all resources, sorted by the given order_by_clause (e.g., "type, network_address").
    pub fn get_all_sorted(
        session: &Session,
        order_by_clause: &str,
        labels: &Vec<Box<str>>,
    ) -> Result<Vec<Resource>, Error> {
        let rows = session.runtime.block_on(async {
            Query::select()
                .columns(vec![Resources::Type, Resources::State, Resources::AvailableUpto])
                .columns(labels.iter().map(|s| Alias::new(s.as_ref())).collect::<Vec<Alias>>())
                .from(Resources::Table)
                .order_by_expr(sea_query::SimpleExpr::Custom(order_by_clause.to_string()), sea_query::Order::Asc)
                .fetch_all(session)
                .await
        })?;

        let mut results = Vec::new();
        for row in rows {
            let mut map = HashMap::new();
            labels.iter().for_each(|label| {
                let value: Result<i64, _> = row.try_get(label.as_ref());
                if let Ok(v) = value {
                    map.insert(label.clone(), ResourceLabelValue::Integer(v));
                } else {
                    let v: String = row
                        .try_get(label.as_ref())
                        .expect(format!("Failed to get resource label value for label {}", label).as_str());
                    map.insert(label.clone(), ResourceLabelValue::Varchar(v));
                }
            });
            results.push(Resource {
                id: row.get("resource_id"),
                r#type: row.get("type"),
                state: row.get("state"),
                available_upto: row.get("available_upto"),
                labels: map,
            });
        }
        Ok(results)
    }
}
