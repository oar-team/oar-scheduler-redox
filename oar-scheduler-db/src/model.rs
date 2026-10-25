pub mod jobs;
pub mod events;
pub mod gantt;
pub mod resources;
pub mod admin;
pub mod queues;
pub mod job_types;
pub mod job_dependencies;
pub mod moldable;
pub mod meta;

pub trait SqlEnum {
    fn as_str(&self) -> &str;
    fn from_str(s: &str) -> Option<Self>
    where
        Self: Sized;
}

/// Cast a PostgreSQL enum column to `text` for reading or comparing it.
///
/// sqlx's `Any` driver does not support PostgreSQL custom types, so columns
/// declared with an enum type (e.g. `jobs.state` is `job_state`,
/// `assigned_resources.assigned_resource_index` is `resource_index`) cannot be
/// decoded as `String`. Casting the column to `text` in SQL works on PostgreSQL
/// and is a no-op on SQLite, where those columns are plain `varchar`.
pub fn enum_as_text<C: sea_query::IntoColumnRef>(column: C) -> sea_query::Expr {
    use sea_query::ExprTrait;
    sea_query::Expr::col(column).cast_as(sea_query::Alias::new("text"))
}
