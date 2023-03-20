

SELECT * from tcp_packets  WHERE source_ip = '10.28.205.43' AND source_port = 49509 AND destination_ip = '202.89.233.96' AND destination_port = 443 order by sequence_number;
WITH numbered_packets AS (
    SELECT *, ROW_NUMBER() OVER (ORDER BY sequence_number ASC) AS packet_number
    FROM tcp_packets
    WHERE source_ip = '10.28.205.43' AND source_port = 49509 AND destination_ip = '202.89.233.96' AND destination_port = 443
)
SELECT 
    np1.packet_number AS missing_packet_number, 
    np1.sequence_number AS missing_packet_sequence_number, 
    np1.sequence_number - np2.sequence_number AS missing_packet_gap
FROM numbered_packets np1
LEFT JOIN numbered_packets np2 ON np1.packet_number = np2.packet_number + 1
WHERE np2.packet_number IS NULL
ORDER BY np1.packet_number;


CREATE TABLE tt
(
    name Text,
    PRIMARY KEY(name)
    )
WITH (
   connector='kafka',
   upsert='true',
   properties.bootstrap.server='localhost:9092',
   topic='test_topic'
) ROW FORMAT UPSERT_AVRO row schema location confluent schema registry 'http://127.0.0.1:8081';

CREATE table  IF NOT EXISTS tcp_packets (
        source_ip TEXT ,
        destination_ip TEXT ,
        source_port INT4 ,
        destination_port  INT4,
        sequence_number BIGINT ,
        acknowledgement_number BIGINT ,
        window_size INT4 ,
        is_fin BOOLEAN 
    )WITH (
   connector='kafka',
   properties.bootstrap.server='localhost:9092',
   topic='tcp'
) ROW FORMAT json;

select * from tcp_packets;

select * from (SELECT COUNT(*) as c, source_ip,SOURCE_port,destination_ip,destination_port,sequence_number from tcp_packets GROUP BY source_ip,SOURCE_port,destination_ip,destination_port,sequence_number) where c > 1;

select count(*) as cnt, destination_ip from tcp_packets GROUP by destination_ip ORDER by cnt DESC;


CREATE source  IF NOT EXISTS ether (
        data BYTEA,
    )WITH (
   connector='kafka',
   properties.bootstrap.server='localhost:9092',
   topic='ether'
) ROW FORMAT json;

SELECT * from ether;

create function gcd(int, int) returns int
language python as gcd using link 'http://localhost:8815';
create function extract_source_ip(bytea) returns varchar
language python as extract_source_ip using link 'http://localhost:8815';
create function extract_source_port(bytea) returns BIGINT
language python as extract_source_port using link 'http://localhost:8815';
create function extract_dst_ip(bytea) returns varchar
language python as extract_dst_ip using link 'http://localhost:8815';
create function extract_dst_port(bytea) returns BIGINT
language python as extract_dst_port using link 'http://localhost:8815';
create function extract_sequence_number(bytea) returns BIGINT
language python as extract_sequence_number using link 'http://localhost:8815';
create function extract_acknowledgement_number(bytea) returns BIGINT
language python as extract_acknowledgement_number using link 'http://localhost:8815';
create function extract_window_size(bytea) returns BIGINT
language python as extract_window_size using link 'http://localhost:8815';
create function extract_is_fin(bytea) returns BOOLEAN
language python as extract_is_fin using link 'http://localhost:8815';


CREATE MATERIALIZED VIEW tcp_mv AS select 
	extract_source_ip(data) as source_ip,
	extract_source_port(data) as source_port,
	extract_dst_ip(data) as dst_ip,
	extract_dst_port(data) as dst_port,	
	extract_sequence_number(data) as sequence_number,
	extract_acknowledgement_number(data) as acknowledgement_number,
	extract_window_size(data) as window_size,
	extract_is_fin(data) as is_fine FROM ether WHERE extract_source_ip(data)  is not null;

SELECT * from tcp_mv;
	
SELECT extract_source_ip(data) as from ether limit 100;
SELECT extract_source_ip(data) as source_ip,
	extract_source_port(data) as source_port,
	extract_dst_ip(data) as dst_ip,
	extract_dst_port(data) as dst_port,	
	extract_sequence_number(data) as sequence_number,
	extract_acknowledgement_number(data) as acknowledgement_number,
	extract_window_size(data) as window_size,
	extract_is_fin(data) as is_fine
	 from (select * from ether where extract_source_ip(data)  is not null  limit 20 offset 10) ;
