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

use crate::model::Jobs;
use crate::{Session, SessionSelectStatement};
use sea_query::{Alias, Expr, ExprTrait, Iden, Query};
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

        let dependant_job_id_alias = Alias::new("dependant_job_id");
        let dependencies = Query::select()
            .columns(vec![
                dependant_job_id_alias.to_string(),
                JobDependencies::RequiredJobId.to_string(),
                Jobs::State.to_string(),
                Jobs::ExitCode.to_string(),
            ])
            .from(JobDependencies::Table)
            .expr_as(Expr::col((JobDependencies::Table, JobDependencies::JobId)), dependant_job_id_alias.clone())
            .inner_join(Jobs::Table, Expr::col((JobDependencies::Table, JobDependencies::RequiredJobId)).equals((Jobs::Table, Jobs::Id)))
            .and_where(Expr::col((JobDependencies::Table, JobDependencies::JobId)).is_in(jobs))
            .and_where(Expr::col((JobDependencies::Table, JobDependencies::Index)).eq("CURRENT"))
            .to_owned()
            .fetch_all(session)
            .await?
            .iter()
            .map(|r| {
                (
                    r.get::<i64, &str>(dependant_job_id_alias.to_string().as_str()),
                    r.get::<i64, &str>(JobDependencies::RequiredJobId.to_string().as_str()),
                    r.get::<String, &str>(Jobs::State.to_string().as_str()).into_boxed_str(),
                    r.try_get::<i32, &str>(Jobs::ExitCode.to_string().as_str()).ok(),
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
