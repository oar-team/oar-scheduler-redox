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

use crate::{Session, SessionSelectStatement};
use sea_query::{Expr, ExprTrait, Iden, Query};
use sqlx::{Error, Row};
use std::collections::HashMap;

#[derive(Iden)]
pub enum JobTypes {
    #[iden = "job_types"]
    Table,
    #[iden = "job_type_id"]
    Id,
    #[iden = "job_id"]
    JobId,
    #[iden = "type"]
    Type,
}

pub struct AllJobTypes {
    types: HashMap<i64, HashMap<Box<str>, Option<Box<str>>>>,
}
impl AllJobTypes {
    pub(crate) async fn load_type_for_jobs(session: &Session, jobs: Vec<i64>) -> Result<Self, Error> {
        if jobs.is_empty() {
            return Ok(Self { types: HashMap::new() });
        }

        let types = Query::select()
            .columns(vec![JobTypes::JobId, JobTypes::Type])
            .from(JobTypes::Table)
            .and_where(Expr::col(JobTypes::JobId).is_in(jobs))
            .to_owned()
            .fetch_all(session)
            .await?
            .iter()
            .map(|r| {
                let job_id = r.get::<i64, &str>(JobTypes::JobId.to_string().as_str());
                let t = r.get::<String, &str>(JobTypes::Type.to_string().as_str());
                let mut t = t.split('=');
                (
                    job_id,
                    t.next().unwrap_or("").to_string().into_boxed_str(),
                    t.next().map(|s| s.to_string().into_boxed_str()),
                )
            })
            .fold(HashMap::new(), |mut acc, (job_id, type_name, type_value)| {
                acc.entry(job_id).or_insert_with(HashMap::new).insert(type_name, type_value);
                acc
            });

        Ok(Self { types })
    }
    pub fn get_job_types(&self, job_id: i64) -> HashMap<Box<str>, Option<Box<str>>> {
        self.types.get(&job_id).unwrap_or(&HashMap::new()).clone()
    }
}
