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
