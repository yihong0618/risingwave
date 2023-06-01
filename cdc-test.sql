CREATE TABLE t (
    v1 varchar,
    v2 interval,
    pk bigint primary key
) WITH (
    connector = 'postgres-cdc',
    hostname = 'localhost',
    port = '5432',
    username = 'bugenzhao',
    password = 'not used',
    database.name = 'postgres',
    schema.name = 'public',
    table.name = 't',
    slot.name = 't'
);


create table t (
    k bigint auto_increment,
    v text,
    primary key (k)
);


CREATE TABLE t (
   k bigint,
   v varchar,
   PRIMARY KEY(k)
) WITH (
    connector = 'mysql-cdc',
    hostname = 'localhost',
    port = '3306',
    username = 'root',
    password = '',
    database.name = 'bz',
    table.name = 't',
    server.id = '2'
);

create table t2 (
    k2 bigint auto_increment,
    v2 text,
    primary key (k2)
);
