-- create database and schemas
create database worldcup;
create schema land;
create schema raw;
create schema clean;
create schema consumption;

show schemas;

use schema land;

-- creating file format
create or replace file format json_format
 type = json
 null_if = ('\\n', 'null', '')
    strip_outer_array = true
    comment = 'Json File Format with outer stip array flag true'; 

-- creating an internal stage
create or replace stage landing_stage; 

-- lets list the internal stage
list @landing_stage;

-- creating a raw table to store all the json data file with root elements extracted
create or replace transient table worldcup.raw.match_raw_tbl (
    meta object not null,
    info variant not null,
    innings ARRAY not null,
);


copy into worldcup.raw.match_raw_tbl from 
    (
    select 
        t.$1:meta::object as meta, 
        t.$1:info::variant as info, 
        t.$1:innings::array as innings
    from @worldcup.land.landing_stage/cricket/json (file_format => 'worldcup.land.json_format') t
    )
    on_error = continue;


SELECT
  info:match_type_number::int AS match_type_number,
  info:match_type::text AS match_type,
  info:season::text AS season,
  info:team_type::text AS team_type,
  info:overs::text AS overs,
  info:city::text AS city,
  info:venue::text AS venue
FROM
  worldcup.raw.match_raw_tbl;

create or replace transient table worldcup.clean.match_detail_clean as
select
    info:match_type_number::int as match_type_number, 
    info:event.name::text as event_name,
    case
    when 
        info:event.match_number::text is not null then info:event.match_number::text
    when 
        info:event.stage::text is not null then info:event.stage::text
    else
        'NA'
    end as match_stage,   
    info:dates[0]::date as event_date,
    date_part('year',info:dates[0]::date) as event_year,
    date_part('month',info:dates[0]::date) as event_month,
    date_part('day',info:dates[0]::date) as event_day,
    info:match_type::text as match_type,
    info:season::text as season,
    info:team_type::text as team_type,
    info:overs::text as overs,
    info:city::text as city,
    info:venue::text as venue, 
    info:gender::text as gender,
    info:teams[0]::text as first_team,
    info:teams[1]::text as second_team,
    case 
        when info:outcome.winner is not null then 'Result Declared'
        when info:outcome.result = 'tie' then 'Tie'
        when info:outcome.result = 'no result' then 'No Result'
        else info:outcome.result
    end as matach_result,
    case 
        when info:outcome.winner is not null then info:outcome.winner
        else 'NA'
    end as winner,   

    info:toss.winner::text as toss_winner,
    initcap(info:toss.decision::text) as toss_decision
    from 
    worldcup.raw.match_raw_tbl;


create or replace table player_clean_tbl as 
select 
    rcm.info:match_type_number::int as match_type_number, 
    p.path::text as country,
    team.value:: text as player_name,
    stg_file_name ,
    stg_file_row_number,
    stg_file_hashkey,
    stg_modified_ts
from worldcup.raw.match_raw_tbl rcm,
lateral flatten (input => rcm.info:players) p,
lateral flatten (input => p.value) team;


-- Adding constraints to ensure data quality
ALTER TABLE worldcup.clean.player_clean_tbl ALTER  match_type_number  NOT NULL;

ALTER TABLE worldcup.clean.player_clean_tbl ALTER  country NOT NULL;

ALTER TABLE worldcup.clean.player_clean_tbl ALTER  player_name NOT NULL;


ALTER TABLE worldcup.clean.match_detail_clean
ADD CONSTRAINT pk_match_type_number PRIMARY KEY (match_type_number);

DESC TABLE worldcup.clean.match_detail_clean;

ALTER TABLE worldcup.clean.player_clean_tbl
ADD CONSTRAINT fk_match_id
FOREIGN KEY (match_type_number)
REFERENCES worldcup.clean.match_detail_clean (match_type_number);



select 
  m.info:match_type_number::int as match_type_number,
  m.innings
from worldcup.raw.match_raw_tbl m
where match_type_number = 4667;

create or replace table delivery_clean_tbl as
select 
    m.info:match_type_number::int as match_type_number, 
    i.value:team::text as country,
    o.value:over::int+1 as over,
    d.value:bowler::text as bowler,
    d.value:batter::text as batter,
    d.value:non_striker::text as non_striker,
    d.value:runs.batter::text as runs,
    d.value:runs.extras::text as extras,
    d.value:runs.total::text as total,
    e.key::text as extra_type,
    e.value::number as extra_runs,
    w.value:player_out::text as player_out,
    w.value:kind::text as player_out_kind,
    w.value:fielders::variant as player_out_fielders
from worldcup.raw.match_raw_tbl m,
lateral flatten (input => m.innings) i,
lateral flatten (input => i.value:overs) o,
lateral flatten (input => o.value:deliveries) d,
lateral flatten (input => d.value:extras, outer => True) e,
lateral flatten (input => d.value:wickets, outer => True) w;

-- Adding constraints
ALTER TABLE worldcup.clean.delivery_clean_tbl
MODIFY COLUMN match_type_number SET NOT NULL;

ALTER TABLE worldcup.clean.delivery_clean_tbl
MODIFY COLUMN country SET NOT NULL;

ALTER TABLE worldcup.clean.delivery_clean_tbl
MODIFY COLUMN over SET NOT NULL;

ALTER TABLE worldcup.clean.delivery_clean_tbl
MODIFY COLUMN bowler SET NOT NULL;

ALTER TABLE worldcup.clean.delivery_clean_tbl
MODIFY COLUMN batter SET NOT NULL;

ALTER TABLE worldcup.clean.delivery_clean_tbl
MODIFY COLUMN non_striker SET NOT NULL;


ALTER TABLE worldcup.clean.delivery_clean_tbl
ADD CONSTRAINT fk_delivery_match_id
FOREIGN KEY (match_type_number)
REFERENCES worldcup.clean.match_detail_clean(match_type_number);

--creating stream 

CREATE
OR REPLACE STREAM worldcup.raw.for_match_stream ON TABLE worldcup.raw.match_raw_tbl APPEND_ONLY = true;

CREATE
OR REPLACE STREAM worldcup.raw.for_player_stream ON TABLE worldcup.raw.match_raw_tbl APPEND_ONLY = true;

CREATE
OR REPLACE STREAM worldcup.raw.for_delivery_stream ON TABLE worldcup.raw.match_raw_tbl APPEND_ONLY = true;

CREATE
OR REPLACE TASK worldcup.raw.load_json_to_raw warehouse = 'COMPUTE_WH' SCHEDULE = '24 hour' AS copy into worldcup.raw.match_raw_tbl
from
    (
        select
            t.$1 :meta :: object as meta,
            t.$1 :info :: variant as info,
            t.$1 :innings :: array as innings
        from
            @worldcup.land.landing_stage/cricket/json(file_format=>'worldcup.land.json_format') t
    ) on_error = continue;


CREATE
OR REPLACE TASK worldcup.raw.load_to_clean_match warehouse = 'COMPUTE_WH'
after
    worldcup.raw.load_json_to_raw when system$stream_has_data('worldcup.raw.for_match_stream') as
insert into
    worldcup.clean.match_detail_clean
select
    info:match_type_number :: int as match_type_number,
    info:event.name :: text as event_name,
    case
        when info:event.match_number :: text is not null then info :event.match_number :: text
        when info:event.stage :: text is not null then info :event.stage :: text
        else 'NA'
    end as match_stage,
    info :dates [0] :: date as event_date,
    date_part('year', info :dates [0] :: date) as event_year,
    date_part('month', info :dates [0] :: date) as event_month,
    date_part('day', info :dates [0] :: date) as event_day,
    info :match_type :: text as match_type,
    info :season :: text as season,
    info :team_type :: text as team_type,
    info :overs :: text as overs,
    info :city :: text as city,
    info :venue :: text as venue,
    info :gender :: text as gender,
    info :teams [0] :: text as first_team,
    info :teams [1] :: text as second_team,
    case
        when info :outcome.winner is not null then 'Result Declared'
        when info :outcome.result = 'tie' then 'Tie'
        when info :outcome.result = 'no result' then 'No Result'
        else info :outcome.result
    end as matach_result,
    case
        when info :outcome.winner is not null then info :outcome.winner
        else 'NA'
    end as winner,
    info :toss.winner :: text as toss_winner,
    initcap(info :toss.decision :: text) as toss_decision
from
    worldcup.raw.for_match_stream;

CREATE
OR REPLACE TASK worldcup.raw.load_to_clean_player WAREHOUSE = 'COMPUTE_WH'
AFTER
    worldcup.raw.load_clean_match
    WHEN SYSTEM$STREAM_HAS_DATA('worldcup.raw.for_player_stream') AS
INSERT INTO
    worldcup.clean.player_clean_tbl
SELECT
    rcm.info :match_type_number :: int as match_type_number,
    p.path :: text as country,
    team.value :: text as player_name
FROM
    worldcup.raw.for_player_stream rcm,
    lateral flatten (input=> rcm.info :players) p,
    lateral flatten (input=> p.value) team;

CREATE
OR REPLACE TASK worldcup.raw.load_to_clean_delivery WAREHOUSE = 'COMPUTE_WH'
AFTER
    worldcup.raw.load_to_clean_player
    WHEN SYSTEM$STREAM_HAS_DATA('worldcup.raw.for_delivery_stream') AS
INSERT INTO
    worldcup.clean.delivery_clean_tbl
SELECT
    m.info :match_type_number :: int as match_type_number,
    i.value :team :: text as country,
    o.value :over :: int + 1 as over,
    d.value :bowler :: text as bowler,
    d.value :batter :: text as batter,
    d.value :non_striker :: text as non_striker,
    d.value :runs.batter :: text as runs,
    d.value :runs.extras :: text as extras,
    d.value :runs.total :: text as total,
    e.key :: text as extra_type,
    e.value :: number as extra_runs,
    w.value :player_out :: text as player_out,
    w.value :kind :: text as player_out_kind,
    w.value :fielders :: variant as player_out_fielders
FROM
    worldcup.raw.for_delivery_stream m,
    lateral flatten (input=> m.innings) i,
    lateral flatten (input=> i.value :overs) o,
    lateral flatten (input=> o.value :deliveries) d,
    lateral flatten (input=> d.value :extras, outer=> True) e,
    lateral flatten (input=> d.value :wickets, outer=> True) w;

ALTER TASK worldcup.raw.load_to_clean_delivery RESUME;

ALTER TASK worldcup.raw.load_to_clean_player RESUME;

ALTER TASK worldcup.raw.load_to_clean_match RESUME;

ALTER TASK worldcup.raw.load_json_to_raw RESUME;
    
