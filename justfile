# set shell := ["bash", "-eu", "-o", "pipefail", "-c"]

pgdata := ".data"
pghost := "127.0.0.1"
pgport := "4321"
pgsocketdir := "/tmp"
pguser := "oar"
pgpassword := "oar"
pgdatabase := "oar"

default:
    @just --list

# Initialize PostgreSQL DB
init_pg:
    #!/usr/bin/env bash
    if [ ! -d "{{pgdata}}" ]; then
    echo "Initialization PostgreSQL in {{pgdata}}"
        initdb -D "{{pgdata}}"

        cat >> "{{pgdata}}/postgresql.conf" <<EOF
    listen_addresses = '*'
    port = {{pgport}}
    unix_socket_directories = '{{pgsocketdir}}'
    EOF

        cat >> "{{pgdata}}/pg_hba.conf" <<EOF
    local   all             all                                     trust
    host    all             all             127.0.0.1/32            md5
    host    all             all             ::1/128                 md5
    EOF

        #pg_ctl -D "{{pgdata}}" -l logfile start
        pg_ctl -D "{{pgdata}}" -l logfile -o "-p {{pgport}} -k {{pgsocketdir}}" start
        sleep 2

        # Create oar db and add oar user
        psql -h {{pgsocketdir}} -p {{pgport}} postgres <<EOF
    CREATE USER {{pguser}} WITH PASSWORD '{{pgpassword}}';
    CREATE DATABASE {{pgdatabase}} OWNER {{pguser}};
    EOF

        #if [ -f schema.sql ]; then
        #  echo "Exec schema.sql in {{pgdatabase}}"
        #  psql -U {{pguser}} -d {{pgdatabase}} -f schema.sql
        #fi

        pg_ctl -D "{{pgdata}}" stop
    else
        echo "PG DB already initialised ({{pgdata}} exists)."
    fi

# Start PostgreSQL server
start_pg:
    pg_ctl -D "{{pgdata}}" -l logfile -o "-p {{pgport}}" start

# Stop PostgreSQL server
stop_pg:
    pg_ctl -D "{{pgdata}}" stop

# Launch PostgreSQL shell
psql:
    psql -h "{{pghost}}" -U "{{pguser}}" -d "{{pgdatabase}}"

rm_db:
    rm -rf "{{pgdata}}"
