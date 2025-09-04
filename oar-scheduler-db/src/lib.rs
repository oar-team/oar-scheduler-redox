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

use crate::model::{resources, NewResource, Resource, ResourceLabelValue};
use log::info;
use oar_scheduler_core::model::configuration::Configuration;
use oar_scheduler_core::platform::{ProcSet, ResourceSet};
use oar_scheduler_core::scheduler::hierarchy::Hierarchy;
use sea_query::{Iden, InsertStatement, PostgresQueryBuilder, QueryBuilder, SelectStatement, SqliteQueryBuilder};
use sea_query_sqlx::{SqlxBinder, SqlxValues};
use sqlx::any::{install_default_drivers, AnyRow};
use sqlx::pool::PoolOptions;
use sqlx::AnyPool;
use sqlx::{Any, Error};
use std::collections::HashMap;
use tokio::runtime::Runtime;

pub mod example;
pub mod model;

enum Backend {
    Postgres,
    Sqlite,
}
impl From<&str> for Backend {
    fn from(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "postgres" | "postgresql" => Backend::Postgres,
            "sqlite" | "sqlite3" => Backend::Sqlite,
            _ => panic!("Unsupported database backend {}", s),
        }
    }
}
impl Backend {
    fn build_insert(&self, query: &InsertStatement) -> (String, SqlxValues) {
        match self {
            Backend::Postgres => query.build_sqlx(PostgresQueryBuilder),
            Backend::Sqlite => query.build_sqlx(SqliteQueryBuilder),
        }
    }
    fn build_select(&self, query: &SelectStatement) -> (String, SqlxValues) {
        match self {
            Backend::Postgres => query.build_sqlx(PostgresQueryBuilder),
            Backend::Sqlite => query.build_sqlx(SqliteQueryBuilder),
        }
    }
}

pub struct Session {
    /// sqlx connection pool.
    pool: AnyPool,
    /// Database backend type (Postgres or Sqlite).
    backend: Backend,
    /// Tokio runtime used to run async database operations in a sync context.
    runtime: Runtime,
    /// Maps the database resource ID to the enumerated ID used in the ProcSet.
    resource_id_to_resource_index: HashMap<i32, u32>,
}

impl Session {
    pub fn new(database_url: &str) -> Session {
        let max_connections = 1; // Only one connection is needed since we are using a single-threaded runtime.
        let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();

        let (pool, backend) = runtime.block_on(async {
            install_default_drivers();

            let pool = PoolOptions::<Any>::new()
                .max_connections(max_connections)
                .connect(database_url)
                .await
                .expect("Failed to create connection pool");

            let conn = pool.acquire().await.expect("Failed to acquire connection");
            let backend = conn.backend_name().into();
            conn.close().await.unwrap();
            (pool, backend)
        });
        let resource_id_to_resource_index = HashMap::new();
        Session { pool, backend, runtime, resource_id_to_resource_index }
    }
    pub fn get_now(&self) -> i64 {
        match self.backend {
            Backend::Postgres => {
                let row: (i64,) = self.runtime.block_on(async {
                    sqlx::query_as("SELECT EXTRACT(EPOCH FROM current_timestamp)::BIGINT")
                        .fetch_one(&self.pool)
                        .await
                        .expect("Failed to fetch current time")
                });
                row.0
            }
            Backend::Sqlite => {
                let row: (i64,) = self.runtime.block_on(async {
                    sqlx::query_as("SELECT CAST(strftime('%s','now') AS INTEGER)")
                        .fetch_one(&self.pool)
                        .await
                        .expect("Failed to fetch current time")
                });
                row.0
            }
        }
    }
    pub fn create_schema(&self) {
        let sql = match self.backend {
            Backend::Postgres => todo!(),
            Backend::Sqlite => include_str!("sql/up-sqlite.sql"),
        };
        self.runtime.block_on(async {
            sqlx::query(sql).execute(&self.pool).await.expect("Failed to create schema");
        });
    }
    pub fn get_resource_set(&mut self, config: &Configuration) -> ResourceSet {
        let mut resource_id_to_resource_index = HashMap::new();
        let labels = config
            .hierarchy_labels
            .clone()
            .map(|s| s.split(',').map(|s| s.trim().to_string().into_boxed_str()).collect())
            .unwrap_or(vec![Box::from("resource_id"), Box::from("network_address")]);

        let order_by = config.scheduler_resource_order.clone().unwrap_or("type, network_address".to_string());
        let resources = Resource::get_all_sorted(&self, order_by.as_str(), &labels).unwrap();
        info!("Loaded {} resources from database", resources.len());
        info!("Resource labels considered: {:?}", labels);

        let suspended_types: Vec<String> = config
            .scheduler_available_suspended_resource_type
            .clone()
            .unwrap_or("".to_string())
            .split(',')
            .map(|s| s.trim().to_string())
            .collect();

        let mut nb_resources_not_dead = 0;
        let mut nb_resources_default_not_dead = 0;
        let mut suspendable_resources = Vec::new();
        let mut default_resources = Vec::new();
        let mut available_upto_map: HashMap<i64, Vec<u32>> = HashMap::new();
        // Mapping: resource label name -> (resource label value -> [enumerated id])
        let mut hierarchy_resources: HashMap<Box<str>, HashMap<ResourceLabelValue, Vec<u32>>> = HashMap::new();

        for (enumerated_id, resource) in resources.iter().enumerate() {
            resource_id_to_resource_index.insert(resource.id, enumerated_id as u32);
            info!("Resource {}: id={} type={}, state={} map={:?}", enumerated_id, resource.id, resource.r#type, resource.state, resource.labels);
            if resource.r#state.to_lowercase() != "dead" {
                nb_resources_not_dead += 1;
                if resource.r#type.to_lowercase() == "default" {
                    nb_resources_default_not_dead += 1;
                }
            }
            if resource.state.to_lowercase() == "alive" || resource.state.to_lowercase() == "absent" {
                if resource.r#type.to_lowercase() == "default" {
                    default_resources.push(enumerated_id as u32);
                }
                for (label, value) in resource.labels.iter() {
                    let entry = hierarchy_resources.entry(label.clone()).or_insert_with(HashMap::new);
                    entry.entry(value.clone()).or_insert_with(Vec::new).push(enumerated_id as u32);
                }
                if let Some(time) = resource.available_upto {
                    available_upto_map.entry(time).or_insert_with(Vec::new).push(enumerated_id as u32);
                }
                if suspended_types.contains(&resource.r#type) {
                    suspendable_resources.push(enumerated_id as u32);
                }
            }
        }

        let mut hierarchy = Hierarchy::new();
        for (label, map) in hierarchy_resources.into_iter() {
            let mut partitions = Vec::new();
            let mut is_unit = true;
            for (_value, ids) in map.into_iter() {
                if ids.len() > 1 {
                    is_unit = false;
                }
                partitions.push(ProcSet::from_iter(ids.iter()));
            }
            hierarchy = if is_unit {
                hierarchy.add_unit_partition(label)
            } else {
                hierarchy.add_partition(label, partitions.into_boxed_slice())
            };
        }

        self.resource_id_to_resource_index = resource_id_to_resource_index;
        ResourceSet {
            nb_resources_not_dead,
            nb_resources_default_not_dead,
            suspendable_resources: ProcSet::from_iter(suspendable_resources.iter()),
            default_resources: ProcSet::from_iter(default_resources.iter()),
            available_upto: available_upto_map
                .into_iter()
                .map(|(time, ids)| (time, ProcSet::from_iter(ids.iter())))
                .collect(),
            hierarchy,
        }
    }
    pub fn resource_id_to_resource_index(&self, resource_id: i32) -> Option<u32> {
        self.resource_id_to_resource_index.get(&resource_id).cloned()
    }
}

trait SessionInsertStatement {
    async fn fetch_one<'q>(&'q self, session: &Session) -> Result<AnyRow, Error>;
    async fn execute<'q>(&'q self, session: &Session) -> Result<u64, Error>;
}
impl SessionInsertStatement for InsertStatement {
    async fn fetch_one<'q>(&'q self, session: &Session) -> Result<AnyRow, Error> {
        let (sql, values) = session.backend.build_insert(&self);
        info!("SQL: {}   VALUES: {:?}", sql, values);
        sqlx::query_with(sql.as_str(), values).fetch_one(&session.pool).await
    }
    async fn execute<'q>(&'q self, session: &Session) -> Result<u64, Error> {
        let (sql, values) = session.backend.build_insert(&self);
        info!("SQL: {}   VALUES: {:?}", sql, values);
        let result = sqlx::query_with(sql.as_str(), values).execute(&session.pool).await?;
        Ok(result.rows_affected())
    }
}
trait SessionSelectStatement {
    async fn fetch_one<'q>(&'q self, session: &Session) -> Result<AnyRow, Error>;
    async fn fetch_all<'q>(&'q self, session: &Session) -> Result<Vec<AnyRow>, Error>;
}
impl SessionSelectStatement for SelectStatement {
    async fn fetch_one<'q>(&'q self, session: &Session) -> Result<AnyRow, Error> {
        let (sql, values) = session.backend.build_select(&self);

        sqlx::query_with(sql.as_str(), values).fetch_one(&session.pool).await
    }
    async fn fetch_all<'q>(&'q self, session: &Session) -> Result<Vec<AnyRow>, Error> {
        let (sql, values) = session.backend.build_select(&self);
        info!("SQL: {}   VALUES: {:?}", sql, values);
        sqlx::query_with(sql.as_str(), values).fetch_all(&session.pool).await
    }
}
