use crate::{Session, SessionInsertStatement, SessionSelectStatement};
use sea_query::{Expr, Iden, Query};
use sqlx::{Error, Row};

#[derive(Iden)]
pub enum Resources {
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
pub enum ResourceLogs {
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
pub enum Files {
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
pub enum Queues {
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

#[derive(Iden)]
pub enum Accounting {
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
}
impl NewResource {
    pub async fn insert(&self, session: &Session) -> Result<i64, Error> {
        let row = Query::insert()
            .into_table(Resources::Table)
            .columns(vec![Resources::NetworkAddress, Resources::Type, Resources::State])
            .values_panic(vec![
                Expr::val(&self.network_address),
                Expr::val(&self.r#type),
                Expr::val(&self.state),
            ])
            .returning_col(Resources::ResourceId)
            .fetch_one(session).await?;

        Ok(row.try_get::<i64, _>(0)?)
    }
}

impl Resources {
    pub async fn get_all_sorted(session: &Session, order_by_clause: &str) -> Result<Vec<(i64, String, String, String)>, Error> {
        let rows = Query::select()
            .columns(vec![Resources::ResourceId, Resources::NetworkAddress, Resources::Type, Resources::State])
            .from(Resources::Table)
            .order_by_expr(
                sea_query::SimpleExpr::Custom(order_by_clause.to_string()),
                sea_query::Order::Asc,
            )
            .fetch_all(session)
            .await?;

        let mut results = Vec::new();
        for row in rows {
            let resource_id: i64 = row.try_get("resource_id")?;
            let network_address: String = row.try_get("network_address")?;
            let r#type: String = row.try_get("type")?;
            let state: String = row.try_get("state")?;
            results.push((resource_id, network_address, r#type, state));
        }
        Ok(results)
    }
}
