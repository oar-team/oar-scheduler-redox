DROP TABLE IF EXISTS resources;
CREATE TABLE resources
(
    resource_id          bigserial,
    type                 varchar(100)                                                                           NOT NULL default 'default',
    network_address      varchar(100)                                                                           NOT NULL default '',
    state                varchar(9) check (state in ('Alive', 'Dead', 'Suspected', 'Absent'))                   NOT NULL default 'Alive',
    next_state           varchar(9) check (next_state in ('UnChanged', 'Alive', 'Dead', 'Absent', 'Suspected')) NOT NULL default 'UnChanged',
    finaud_decision      varchar(3) check (finaud_decision in ('YES', 'NO'))                                    NOT NULL default 'NO',
    next_finaud_decision varchar(3) check (next_finaud_decision in ('YES', 'NO'))                               NOT NULL default 'NO',
    state_num            integer                                                                                NOT NULL default '0',
    suspended_jobs       varchar(3) check (suspended_jobs in ('YES', 'NO'))                                     NOT NULL default 'NO',
    scheduler_priority   integer                                                                                NOT NULL default '0',
    cpuset               varchar(255)                                                                           NOT NULL default '0',
    besteffort           varchar(3) check (besteffort in ('YES', 'NO'))                                         NOT NULL default 'YES',
    deploy               varchar(3) check (deploy in ('YES', 'NO'))                                             NOT NULL default 'NO',
    expiry_date          integer                                                                                NOT NULL default '0',
    desktop_computing    varchar(3) check (desktop_computing in ('YES', 'NO'))                                  NOT NULL default 'NO',
    last_job_date        integer                                                                                NOT NULL default '0',
    available_upto       integer                                                                                NOT NULL default '2147483647',
    last_available_upto  integer                                                                                NOT NULL default '0',
    drain                varchar(3) check (drain in ('YES', 'NO'))                                              NOT NULL default 'NO',
    PRIMARY KEY (resource_id)
);
CREATE INDEX resource_state ON resources (state);
CREATE INDEX resource_next_state ON resources (next_state);
CREATE INDEX resource_suspended_jobs ON resources (suspended_jobs);
CREATE INDEX resource_type ON resources (type);
CREATE INDEX resource_network_address ON resources (network_address);
