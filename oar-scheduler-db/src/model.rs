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

pub mod jobs;
pub mod events;
pub mod gantt;
pub mod resources;
pub mod admin;
pub mod queues;
pub mod job_types;
pub mod job_dependencies;
pub mod moldable;

pub trait SqlEnum {
    fn as_str(&self) -> &str;
    fn from_str(s: &str) -> Option<Self>
    where
        Self: Sized;
}
