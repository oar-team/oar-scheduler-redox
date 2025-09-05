use crate::model::jobs::Jobs;
use crate::{Session, SessionSelectStatement};
use sea_query::{Expr, ExprTrait, Iden, Query};
use sqlx::{Error, Row};
use std::collections::HashMap;

#[derive(Iden)]
pub enum JobDependencies {
    #[iden = "job_dependencies"]
    Table,
    #[iden = "job_id"]
    JobId,
    #[iden = "job_id_required"]
    RequiredJobId,
    #[iden = "job_dependency_index"]
    Index,
}

pub struct AllJobDependencies {
    dependencies: HashMap<i64, Vec<(i64, Box<str>, Option<i32>)>>,
}
impl AllJobDependencies {
    pub(crate) async fn load_dependencies_for_jobs(session: &Session, jobs: Vec<i64>) -> Result<Self, Error> {
        if jobs.is_empty() {
            return Ok(Self {
                dependencies: HashMap::new(),
            });
        }

        let dependencies = Query::select()
            .columns(vec![
                JobDependencies::RequiredJobId.to_string(),
                Jobs::State.to_string(),
                Jobs::ExitCode.to_string(),
            ])
            .columns(vec![
                (JobDependencies::Table, JobDependencies::JobId)
            ])
            .from(JobDependencies::Table)
            .inner_join(Jobs::Table, Expr::col((JobDependencies::Table, JobDependencies::RequiredJobId)).equals((Jobs::Table, Jobs::Id)))
            .and_where(Expr::col((JobDependencies::Table, JobDependencies::JobId)).is_in(jobs))
            .and_where(Expr::col((JobDependencies::Table, JobDependencies::Index)).eq("CURRENT"))
            .to_owned()
            .fetch_all(session)
            .await?
            .iter()
            .map(|r| {
                (
                    r.get::<i64, &str>(JobDependencies::JobId.unquoted()),
                    r.get::<i64, &str>(JobDependencies::RequiredJobId.unquoted()),
                    r.get::<String, &str>(Jobs::State.unquoted()).into_boxed_str(),
                    r.try_get::<i32, &str>(Jobs::ExitCode.unquoted()).ok(),
                )
            })
            .fold(
                HashMap::<i64, Vec<(i64, Box<str>, Option<i32>)>>::new(),
                |mut acc, (job_id, job_required_id, job_required_state, job_required_exit_code)| {
                    acc.entry(job_id)
                        .or_insert_with(Vec::new)
                        .push((job_required_id, job_required_state, job_required_exit_code));
                    acc
                },
            );
        Ok(Self { dependencies })
    }
    pub fn get_job_dependencies(&self, job_id: i64) -> Vec<(i64, Box<str>, Option<i32>)> {
        self.dependencies.get(&job_id).cloned().unwrap_or_default()
    }
}
