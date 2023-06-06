create type t1_type as (
    k1 int,
    v1 varchar
);

create type t2_type as (
    k2 int,
    v2 varchar
);

create table mycdc(
    t1_col t1_type,
    t2_col t2_type,
    table_name varchar
) partition by list (table_name);

insert into mycdc(table_name) values ('t1');

create table t1 partition of mycdc (
    k1 int generated always as ((t1_col).k1) stored,
    v1 varchar generated always as ((t1_col).v1) stored,
    PRIMARY KEY (k1)
) for values in ('t1');





CREATE SOURCE my_cdc_db WITH (
    connector = 'postgres-cdc',
    hostname = 'postgres',
    port = '5432',
    username = 'myuser',
    password = '123456',
    database.name = 'mydb',
    table.name = 'public.t1, public.t2'
);

BEGIN;

CREATE TABLE t1 (
    k1 int PRIMARY KEY,
    v1 varchar
) FROM SOURCE my_cdc_db TABLE 'public.t1';

CREATE TABLE t2 (
    k2 int PRIMARY KEY,
    v2 varchar
) FROM SOURCE my_cdc_db TABLE 'public.t2';

COMMIT;

-- will be desugared to:

CREATE SOURCE my_cdc_db (
    _public_t1 struct<k1 int, v1 varchar>, -- resolved from Postgres
    _public_t2 struct<k2 int, v2 varchar>,
    table_name varchar
) WITH (
    connector = 'postgres-cdc',
    hostname = 'postgres',
    port = '5432',
    username = 'myuser',
    password = '123456',
    database.name = 'mydb',
    table.name = 'public.t1, public.t2'
);

BEGIN;

CREATE MATERIALIZED VIEW t1 (
    k1 int PRIMARY KEY, -- suppose this is supported
    v1 varchar
) AS
SELECT (_public_t1).k1, (_public_t1).v1 FROM my_cdc_db WHERE table_name = 'public.t1';

CREATE MATERIALIZED VIEW t2 (
    k2 int PRIMARY KEY, -- suppose this is supported
    v2 varchar
) AS
SELECT (_public_t2).k2, (_public_t2).v2 FROM my_cdc_db WHERE table_name = 'public.t2';

COMMIT;




-- schema change

ALTER TABLE t1 ADD COLUMN d1 decimal;

-- will be desugared to:

