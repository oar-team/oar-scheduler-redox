use crate::{Session, SessionInsertStatement};
use sea_query::Iden;

#[derive(Iden)]
pub enum EventLogs {
    #[iden = "event_logs"]
    Table,
    #[iden = "event_id"]
    EventId,
    #[iden = "type"]
    Type,
    #[iden = "job_id"]
    JobId,
    #[iden = "date"]
    Date,
    #[iden = "description"]
    Description,
    #[iden = "to_check"]
    ToCheck,
}

#[derive(Iden)]
pub enum EventLogHostnames {
    #[iden = "event_log_hostnames"]
    Table,
    #[iden = "event_id"]
    EventId,
    #[iden = "hostname"]
    Hostname,
}

pub fn add_new_event(session: &Session, ev_type: &str, job_id: i64, description: &str) {
    add_new_event_with_to_check(session, ev_type, job_id, description, "YES");
}

/// Same as [`add_new_event`] but lets the caller choose the `to_check` value.
pub fn add_new_event_with_to_check(session: &Session, ev_type: &str, job_id: i64, description: &str, to_check: &str) {
    let now = session.get_now();
    session.runtime.block_on(async {
        sea_query::Query::insert()
            .into_table(EventLogs::Table)
            .columns(vec![EventLogs::Type, EventLogs::JobId, EventLogs::Date, EventLogs::Description, EventLogs::ToCheck])
            .values_panic(vec![
                ev_type.into(),
                job_id.into(),
                now.into(),
                description.into(),
                to_check.into(),
            ])
            .execute(session)
            .await
            .expect("Failed to insert new event log");
    });
}
