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
