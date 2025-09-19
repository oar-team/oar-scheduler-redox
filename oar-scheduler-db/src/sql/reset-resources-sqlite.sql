DROP TABLE IF EXISTS resources;

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
