use crate::model::{resources, NewResource, Resources};
use log::info;
use oar_scheduler_core::platform::{ProcSet, ResourceSet};
use oar_scheduler_core::scheduler::hierarchy::Hierarchy;
use sea_query::{InsertStatement, PostgresQueryBuilder, QueryBuilder, SelectStatement, SqliteQueryBuilder};
use sea_query_sqlx::{SqlxBinder, SqlxValues};
use sqlx::any::{install_default_drivers, AnyRow};
use sqlx::pool::PoolOptions;
use sqlx::AnyPool;
use sqlx::{Any, Error};

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
    pool: AnyPool,
    backend: Backend,
}
impl Session {
    pub async fn get_resource_set(&self) -> ResourceSet {

        // Test to create a dummy resource
        NewResource {
            network_address: "test".to_string(),
            r#type: "test".to_string(),
            state: "active".to_string(),
        }.insert(&self).await;

        let resources = Resources::get_all_sorted(&self, "type, network_address").await.unwrap();
        info!("Loaded {} resources from database", resources.len());
        for (resource_id, network_address, r#type, state) in &resources {
            info!("Resource {}: {} (type={}, state={})", resource_id, network_address, r#type, state);
        }


        let hierarchy = Hierarchy::new().add_unit_partition(Box::from("resource_id"));
        ResourceSet {
            default_intervals: ProcSet::default(),
            available_upto: vec![],
            hierarchy,
        }
    }
}

impl Session {
    pub async fn new(database_url: &str, max_connections: u32) -> Session {
        install_default_drivers();

        let pool = PoolOptions::<Any>::new()
            .max_connections(max_connections)
            .connect(database_url)
            .await
            .expect("Failed to create connection pool");

        let conn = pool.acquire().await.expect("Failed to acquire connection");
        let backend = conn.backend_name().into();
        conn.close().await.unwrap();

        Session {
            pool,
            backend,
        }
    }
    pub async fn get_now(&self) -> i64 {
        match self.backend {
            Backend::Postgres => {
                let row: (i64,) = sqlx::query_as("SELECT EXTRACT(EPOCH FROM current_timestamp)::BIGINT")
                    .fetch_one(&self.pool)
                    .await
                    .expect("Failed to fetch current time");
                row.0
            }
            Backend::Sqlite => {
                let row: (i64,) = sqlx::query_as("SELECT CAST(strftime('%s','now') AS INTEGER)")
                    .fetch_one(&self.pool)
                    .await
                    .expect("Failed to fetch current time");
                row.0
            }
        }
    }
}

trait SessionInsertStatement {
    async fn fetch_one<'q>(&'q self, session: &Session) -> Result<AnyRow, Error>;
}
impl SessionInsertStatement for InsertStatement {
    async fn fetch_one<'q>(&'q self, session: &Session) -> Result<AnyRow, Error> {
        let (sql, values) = session.backend.build_insert(&self);
        sqlx::query_with(sql.as_str(), values)
            .fetch_one(&session.pool).await
    }
}
trait SessionSelectStatement {
    async fn fetch_one<'q>(&'q self, session: &Session) -> Result<AnyRow, Error>;
    async fn fetch_all<'q>(&'q self, session: &Session) -> Result<Vec<AnyRow>, Error>;
}
impl SessionSelectStatement for SelectStatement {
    async fn fetch_one<'q>(&'q self, session: &Session) -> Result<AnyRow, Error> {
        let (sql, values) = session.backend.build_select(&self);
        sqlx::query_with(sql.as_str(), values)
            .fetch_one(&session.pool).await
    }
    async fn fetch_all<'q>(&'q self, session: &Session) -> Result<Vec<AnyRow>, Error> {
        let (sql, values) = session.backend.build_select(&self);
        sqlx::query_with(sql.as_str(), values)
            .fetch_all(&session.pool).await
    }
}
