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

CREATE TABLE accounting
(
    window_start       BIGINT                       NOT NULL,
    window_stop        BIGINT       DEFAULT '0'     NOT NULL,
    accounting_user    VARCHAR(255) DEFAULT ''      NOT NULL,
    accounting_project VARCHAR(255) DEFAULT ''      NOT NULL,
    queue_name         VARCHAR(100) DEFAULT ''      NOT NULL,
    consumption_type   VARCHAR(5)   DEFAULT 'ASKED' NOT NULL,
    consumption        BIGINT       DEFAULT '0',
    PRIMARY KEY (window_start, window_stop, accounting_user, accounting_project, queue_name, consumption_type)
);

CREATE TABLE admission_rules
(
    id       INTEGER NOT NULL,
    rule     TEXT,
    priority INTEGER    DEFAULT '0',
    enabled  VARCHAR(3) DEFAULT 'YES',
    PRIMARY KEY (id)
);

CREATE TABLE assigned_resources
(
    moldable_job_id         INTEGER    DEFAULT '0' NOT NULL,
    resource_id             INTEGER    DEFAULT '0' NOT NULL,
    assigned_resource_index VARCHAR(7) DEFAULT 'CURRENT',
    PRIMARY KEY (moldable_job_id, resource_id)
);

CREATE TABLE challenges
(
    job_id          INTEGER      DEFAULT '0' NOT NULL,
    challenge       VARCHAR(255) DEFAULT '',
    ssh_private_key TEXT         DEFAULT '',
    ssh_public_key  TEXT         DEFAULT '',
    PRIMARY KEY (job_id)
);

CREATE TABLE event_logs
(
    event_id    INTEGER NOT NULL,
    type        VARCHAR(100) DEFAULT '',
    job_id      INTEGER      DEFAULT '0',
    date        INTEGER      DEFAULT '0',
    description VARCHAR(255) DEFAULT '',
    to_check    VARCHAR(3)   DEFAULT 'YES',
    PRIMARY KEY (event_id)
);

CREATE TABLE event_log_hostnames
(
    event_id INTEGER      DEFAULT '0' NOT NULL,
    hostname VARCHAR(255) DEFAULT ''  NOT NULL,
    PRIMARY KEY (event_id, hostname)
);

CREATE TABLE files
(
    file_id     INTEGER NOT NULL,
    md5sum      VARCHAR(255) DEFAULT NULL,
    location    VARCHAR(255) DEFAULT NULL,
    method      VARCHAR(255) DEFAULT NULL,
    compression VARCHAR(255) DEFAULT NULL,
    size        INTEGER      DEFAULT '0',
    PRIMARY KEY (file_id)
);

CREATE TABLE frag_jobs
(
    frag_id_job INTEGER     DEFAULT '0' NOT NULL,
    frag_date   INTEGER     DEFAULT '0',
    frag_state  VARCHAR(16) DEFAULT 'LEON',
    PRIMARY KEY (frag_id_job)
);

CREATE TABLE gantt_jobs_predictions
(
    moldable_job_id INTEGER DEFAULT '0' NOT NULL,
    start_time      INTEGER DEFAULT '0',
    PRIMARY KEY (moldable_job_id)
);

CREATE TABLE gantt_jobs_predictions_log
(
    sched_date      INTEGER DEFAULT '0' NOT NULL,
    moldable_job_id INTEGER DEFAULT '0' NOT NULL,
    start_time      INTEGER DEFAULT '0',
    PRIMARY KEY (sched_date, moldable_job_id)
);

CREATE TABLE gantt_jobs_predictions_visu
(
    moldable_job_id INTEGER DEFAULT '0' NOT NULL,
    start_time      INTEGER DEFAULT '0',
    PRIMARY KEY (moldable_job_id)
);

CREATE TABLE gantt_jobs_resources
(
    moldable_job_id INTEGER DEFAULT '0' NOT NULL,
    resource_id     INTEGER DEFAULT '0' NOT NULL,
    PRIMARY KEY (moldable_job_id, resource_id)
);

CREATE TABLE gantt_jobs_resources_log
(
    sched_date      INTEGER DEFAULT '0' NOT NULL,
    moldable_job_id INTEGER DEFAULT '0' NOT NULL,
    resource_id     INTEGER DEFAULT '0' NOT NULL,
    PRIMARY KEY (sched_date, moldable_job_id, resource_id)
);

CREATE TABLE gantt_jobs_resources_visu
(
    moldable_job_id INTEGER DEFAULT '0' NOT NULL,
    resource_id     INTEGER DEFAULT '0' NOT NULL,
    PRIMARY KEY (moldable_job_id, resource_id)
);

CREATE TABLE jobs
(
    job_id                INTEGER NOT NULL,
    array_id              INTEGER      DEFAULT '0',
    array_index           INTEGER      DEFAULT '1',
    initial_request       TEXT,
    job_name              VARCHAR(100),
    job_env               TEXT,
    job_type              VARCHAR(11)  DEFAULT 'PASSIVE',
    info_type             VARCHAR(255) DEFAULT NULL,
    state                 VARCHAR(16)  DEFAULT 'Waiting',
    reservation           VARCHAR(10)  DEFAULT 'None',
    message               VARCHAR(255) DEFAULT '',
    scheduler_info        VARCHAR(255) DEFAULT '',
    job_user              VARCHAR(255) DEFAULT '',
    project               VARCHAR(255) DEFAULT '',
    job_group             VARCHAR(255) DEFAULT '',
    command               TEXT,
    exit_code             INTEGER,
    queue_name            VARCHAR(100) DEFAULT '',
    properties            TEXT,
    launching_directory   TEXT,
    submission_time       INTEGER      DEFAULT '0',
    start_time            INTEGER      DEFAULT '0',
    stop_time             INTEGER      DEFAULT '0',
    file_id               INTEGER,
    accounted             VARCHAR(3)   DEFAULT 'NO',
    notify                VARCHAR(255) DEFAULT NULL,
    assigned_moldable_job INTEGER      DEFAULT '0',
    checkpoint            INTEGER      DEFAULT '0',
    checkpoint_signal     INTEGER,
    stdout_file           TEXT,
    stderr_file           TEXT,
    resubmit_job_id       INTEGER      DEFAULT '0',
    suspended             VARCHAR(3)   DEFAULT 'NO',
    last_karma            FLOAT,
    PRIMARY KEY (job_id)
);

CREATE TABLE job_dependencies
(
    job_id               INTEGER    DEFAULT '0' NOT NULL,
    job_id_required      INTEGER    DEFAULT '0' NOT NULL,
    job_dependency_index VARCHAR(7) DEFAULT 'CURRENT',
    PRIMARY KEY (job_id, job_id_required)
);

CREATE TABLE job_resource_descriptions
(
    res_job_group_id      INTEGER      DEFAULT '0' NOT NULL,
    res_job_resource_type VARCHAR(255) DEFAULT ''  NOT NULL,
    res_job_value         INTEGER      DEFAULT '0',
    res_job_order         INTEGER      DEFAULT '0' NOT NULL,
    res_job_index         VARCHAR(7)   DEFAULT 'CURRENT',
    PRIMARY KEY (res_job_group_id, res_job_resource_type, res_job_order)
);

CREATE TABLE job_resource_groups
(
    res_group_id          INTEGER NOT NULL,
    res_group_moldable_id INTEGER    DEFAULT '0',
    res_group_property    TEXT,
    res_group_index       VARCHAR(7) DEFAULT 'CURRENT',
    PRIMARY KEY (res_group_id)
);

CREATE TABLE job_state_logs
(
    job_state_log_id INTEGER NOT NULL,
    job_id           INTEGER     DEFAULT '0',
    job_state        VARCHAR(16) DEFAULT 'Waiting',
    date_start       INTEGER     DEFAULT '0',
    date_stop        INTEGER     DEFAULT '0',
    PRIMARY KEY (job_state_log_id)
);

CREATE TABLE job_types
(
    job_type_id INTEGER NOT NULL,
    job_id      INTEGER      DEFAULT '0',
    type        VARCHAR(255) DEFAULT '',
    types_index VARCHAR(7)   DEFAULT 'CURRENT',
    PRIMARY KEY (job_type_id)
);

CREATE TABLE moldable_job_descriptions
(
    moldable_id       INTEGER NOT NULL,
    moldable_job_id   INTEGER    DEFAULT '0',
    moldable_walltime INTEGER    DEFAULT '0',
    moldable_index    VARCHAR(7) DEFAULT 'CURRENT',
    PRIMARY KEY (moldable_id)
);

CREATE TABLE queues
(
    queue_name       VARCHAR(100) DEFAULT '' NOT NULL,
    priority         INTEGER      DEFAULT '0',
    scheduler_policy VARCHAR(100) DEFAULT '',
    state            VARCHAR(9)   DEFAULT 'Active',
    PRIMARY KEY (queue_name)
);

CREATE TABLE resources
(
    resource_id          INTEGER NOT NULL,
    type                 VARCHAR(100) DEFAULT 'default',
    network_address      VARCHAR(100) DEFAULT '',
    state                VARCHAR(9)   DEFAULT 'Alive',
    next_state           VARCHAR(9)   DEFAULT 'UnChanged',
    finaud_decision      VARCHAR(3)   DEFAULT 'NO',
    next_finaud_decision VARCHAR(3)   DEFAULT 'NO',
    state_num            INTEGER      DEFAULT '0',
    suspended_jobs       VARCHAR(3)   DEFAULT 'NO',
    scheduler_priority   BIGINT       DEFAULT '0',
    cpuset               VARCHAR(255) DEFAULT '0',
    besteffort           VARCHAR(3)   DEFAULT 'YES',
    deploy               VARCHAR(3)   DEFAULT 'NO',
    expiry_date          INTEGER      DEFAULT '0',
    desktop_computing    VARCHAR(3)   DEFAULT 'NO',
    last_job_date        INTEGER      DEFAULT '0',
    available_upto       INTEGER      DEFAULT '2147483647',
    last_available_upto  INTEGER      DEFAULT '0',
    drain                VARCHAR(3)   DEFAULT 'NO',
    PRIMARY KEY (resource_id)
);

CREATE TABLE resource_logs
(
    resource_log_id INTEGER NOT NULL,
    resource_id     INTEGER      DEFAULT '0',
    attribute       VARCHAR(255) DEFAULT '',
    value           VARCHAR(255) DEFAULT '',
    date_start      INTEGER      DEFAULT '0',
    date_stop       INTEGER      DEFAULT '0',
    finaud_decision VARCHAR(3)   DEFAULT 'NO',
    PRIMARY KEY (resource_log_id)
);

CREATE TABLE scheduler
(
    name        VARCHAR(100) NOT NULL,
    script      VARCHAR(100),
    description VARCHAR(255),
    PRIMARY KEY (name)
);

CREATE TABLE walltime_change
(
    job_id                       INTEGER    DEFAULT '0' NOT NULL,
    pending                      INTEGER    DEFAULT '0',
    force                        VARCHAR(3) DEFAULT 'NO',
    delay_next_jobs              VARCHAR(3) DEFAULT 'NO',
    granted                      INTEGER    DEFAULT '0',
    granted_with_force           INTEGER    DEFAULT '0',
    granted_with_delay_next_jobs INTEGER    DEFAULT '0',
    PRIMARY KEY (job_id),
    CHECK (force IN ('NO', 'YES')),
    CHECK (delay_next_jobs IN ('NO', 'YES'))
);
