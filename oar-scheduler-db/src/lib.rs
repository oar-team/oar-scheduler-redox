use oar_scheduler_core::platform::ResourceSet;
use sqlx::any::install_default_drivers;
use sqlx::pool::PoolOptions;
use sqlx::Any;
use sqlx::AnyPool;

pub mod example;

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

pub struct Session {
    pool: AnyPool,
    backend: Backend,
}

impl Session {
    pub async fn get_resource_set(&self) -> ResourceSet {
        todo!()
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
                let row: (i64,) = sqlx::query_as("SELECT EXTRACT(EPOCH FROM current_timestamp)")
                    .fetch_one(&self.pool)
                    .await
                    .expect("Failed to fetch current time");
                row.0
            }
            Backend::Sqlite => {
                let row: (i64,) = sqlx::query_as("SELECT strftime('%s','now')")
                    .fetch_one(&self.pool)
                    .await
                    .expect("Failed to fetch current time");
                row.0
            }
        }
    }
}
