!set force on
ALTER PUMP "ucp".* stop;
DROP SCHEMA "ucp" CASCADE;
!set force off
CREATE OR REPLACE SCHEMA "ucp";

SET SCHEMA '"ucp"';


CREATE OR REPLACE FOREIGN STREAM "control_plane_fs"
(
    "arrival_time" TIMESTAMP not null,
    "user_id" VARCHAR(8),
    "cell_id" VARCHAR(8)
)

    SERVER "FILE_SERVER"

OPTIONS (
"PARSER" 'CSV',
        "CHARACTER_ENCODING" 'UTF-8',
        "SEPARATOR" ',',
        "SKIP_HEADER" 'true',
        "DIRECTORY" '/home/sqlstream/ucp',
        "FILENAME_PATTERN" 'cp.csv'

);


-- CREATE OR REPLACE STREAM "control_plane_ns"
-- (
--     "user_id" VARCHAR(8),
--     "cell_id" VARCHAR(8),
--     "arrival_time" TIMESTAMP
-- );


-- CREATE OR REPLACE PUMP "control_plane_results" STOPPED AS
-- INSERT INTO "control_plane_ns" 
-- ("arrival_time", "user_id","cell_id")
-- select stream "arrival_time","user_id","cell_id"
-- from "control_plane_fs";


CREATE OR REPLACE FOREIGN STREAM "flow_fs"
(
    "flow_start_time" TIMESTAMP NOT NULL,
    "flow_end_time" TIMESTAMP NOT NULL,
    "flow_id" VARCHAR(8),
    "user_id" VARCHAR(8),
    "bytes" BIGINT
)
SERVER "FILE_SERVER"
OPTIONS (
"PARSER" 'CSV',
        "CHARACTER_ENCODING" 'UTF-8',
        "SEPARATOR" ',',
        "SKIP_HEADER" 'true',
        "DIRECTORY" '/home/sqlstream/ucp',
        "FILENAME_PATTERN" 'flow.csv'

);

-- The number of rows in this view should match:
-- * the number of lags in cp_step_2
-- * the unpivot in flow_cell_3
-- The number effectively sets a limit on how many times the user can change cell position during a flow

CREATE OR REPLACE VIEW cellsplitter ("dummy","rowno")
AS SELECT 1,* from(
    VALUES(0),(1),(2),(3),(4),(5),(6),(7),(8),(9),(10),(11),(12),(13),(14),(15)
    ,(16),(17),(18),(19)
);

-- The number of rows in this view sets a limit on the duration of each subflow.
-- The longest sub-flow can be just N * 30 minutes (where 30m is the interval used in flow_intervals_1)
-- For a stationary user, that means the longest flow is also N * 30 minutes
-- With N=8 and interval of 30 mins we support a 4 hour flow

CREATE OR REPLACE VIEW flowsplitter ("dummy","rowno")
AS SELECT 1,* from(
    VALUES(0),(1),(2),(3),(4),(5),(6),(7)
);


-- CREATE OR REPLACE STREAM "flow_ns"
-- (
--     "flow_start_time" TIMESTAMP NOT NULL,
--     "flow_end_time" TIMESTAMP NOT NULL,
--     "flow_id" VARCHAR(8),
--     "user_id" VARCHAR(8),
--     "bytes" BIGINT
-- );

-- CREATE OR REPLACE PUMP "flow_results" STOPPED AS
-- INSERT INTO "flow_ns" 
--     SELECT STREAM *   
--     FROM "flow_fs";

-- get history of cell positions 

    
create or replace view cp_step_1 as
select stream "arrival_time" as rowtime, * 
from "control_plane_fs";

-- the range interval used in the cp_step_2 join should support the longest flow allowed

create or replace view cp_step_2 as
select stream "user_id"
     , "cell_id" as "cell_00"
     , lag("cell_id",1) over W as "cell_01"
     , lag("cell_id",2) over W as "cell_02"
     , lag("cell_id",3) over W as "cell_03"
     , lag("cell_id",4) over W as "cell_04"
     , lag("cell_id",5) over W as "cell_05"
     , lag("cell_id",6) over W as "cell_06"
     , lag("cell_id",7) over W as "cell_07"
     , lag("cell_id",8) over W as "cell_08"
     , lag("cell_id",9) over W as "cell_09"
     , lag("cell_id",10) over W as "cell_10"
     , lag("cell_id",11) over W as "cell_11"
     , lag("cell_id",12) over W as "cell_12"
     , lag("cell_id",13) over W as "cell_13"
     , lag("cell_id",14) over W as "cell_14"
     , lag("cell_id",15) over W as "cell_15"
     , lag("cell_id",16) over W as "cell_16"
     , lag("cell_id",17) over W as "cell_17"
     , lag("cell_id",18) over W as "cell_18"
     , lag("cell_id",19) over W as "cell_19"
     , "arrival_time" as "artm_00"
     , lag("arrival_time",1) over W as "artm_01"
     , lag("arrival_time",2) over W as "artm_02"
     , lag("arrival_time",3) over W as "artm_03"
     , lag("arrival_time",4) over W as "artm_04"
     , lag("arrival_time",5) over W as "artm_05"
     , lag("arrival_time",6) over W as "artm_06"
     , lag("arrival_time",7) over W as "artm_07"
     , lag("arrival_time",8) over W as "artm_08"
     , lag("arrival_time",9) over W as "artm_09"
     , lag("arrival_time",10) over W as "artm_10"
     , lag("arrival_time",11) over W as "artm_11"
     , lag("arrival_time",12) over W as "artm_12"
     , lag("arrival_time",13) over W as "artm_13"
     , lag("arrival_time",14) over W as "artm_14"
     , lag("arrival_time",15) over W as "artm_15"
     , lag("arrival_time",16) over W as "artm_16"
     , lag("arrival_time",17) over W as "artm_17"
     , lag("arrival_time",18) over W as "artm_18"
     , lag("arrival_time",19) over W as "artm_19"
from cp_step_1 s 
window W as (partition by "user_id" range interval '24' hour preceding)
;

create or replace view flow_step_1 as
select stream "flow_end_time" as rowtime, * 
from "flow_fs";

create or replace view flow_cell_1 AS
select stream f."flow_id"
            , f."flow_start_time"
            , f."flow_end_time"
            , f."bytes"
            , 1 as "dummy"
            , cp.*
from  flow_step_1 f
JOIN cp_step_2 over (partition by "user_id" rows current row) cp on cp."user_id" = f."user_id"
;

create or replace view flow_cell_2 AS
select stream f.*,s."rowno"
from flow_cell_1 f JOIN cellsplitter s using ("dummy")
;

create or replace view flow_cell_3 as
select stream f."user_id" 
   , f."flow_id"
   , f."flow_start_time"
   , f."flow_end_time"
   , f."bytes" as "flow_bytes"
   , tsdiff(f."flow_end_time",f."flow_start_time") as "flow_ms"
   , f."flow_id"||'-'||cast("rowno" as varchar(4)) as "sub_flow_id"
   , "rowno"
   , case "rowno"
     when 0 then "cell_00"
     when 1 then "cell_01"
     when 2 then "cell_02"
     when 3 then "cell_03"
     when 4 then "cell_04"
     when 5 then "cell_05"
     when 6 then "cell_06"
     when 7 then "cell_07"
     when 8 then "cell_08"
     when 9 then "cell_09"
     when 10 then "cell_10"
     when 11 then "cell_11"
     when 12 then "cell_12"
     when 13 then "cell_13"
     when 14 then "cell_14"
     when 15 then "cell_15"
     when 16 then "cell_16"
     when 17 then "cell_17"
     when 18 then "cell_18"
     when 19 then "cell_19"
     END as "cell_id"
   , case "rowno"
     when 0 then "artm_00"
     when 1 then "artm_01"
     when 2 then "artm_02"
     when 3 then "artm_03"
     when 4 then "artm_04"
     when 5 then "artm_05"
     when 6 then "artm_06"
     when 7 then "artm_07"
     when 8 then "artm_08"
     when 9 then "artm_09"
     when 10 then "artm_10"
     when 11 then "artm_11"
     when 12 then "artm_12"
     when 13 then "artm_13"
     when 14 then "artm_14"
     when 15 then "artm_15"
     when 16 then "artm_16"
     when 17 then "artm_17"
     when 18 then "artm_18"
     when 19 then "artm_19"
     END as "cell_arrival_time"
   , case "rowno"
     when 0 then "flow_end_time"
     when 1 then "artm_00"
     when 2 then "artm_01"
     when 3 then "artm_02"
     when 4 then "artm_03"
     when 5 then "artm_04"
     when 6 then "artm_05"
     when 7 then "artm_06"
     when 8 then "artm_07"
     when 9 then "artm_08"
     when 10 then "artm_09"
     when 11 then "artm_10"
     when 12 then "artm_11"
     when 13 then "artm_12"
     when 14 then "artm_13"
     when 15 then "artm_14"
     when 16 then "artm_15"
     when 17 then "artm_16"
     when 18 then "artm_17"
     when 19 then "artm_18"
     END as "cell_departure_time"
from  flow_cell_2 f;

-- we have to materialize this so we can define the sub-flow start/end times as mandatory (so STEP function works)
-- TODO we could use millisecond arithmetic to achieve the same goal (with less clarity)

create or replace stream sub_flows 
( "user_id" VARCHAR(8)
, "flow_id" VARCHAR(8)
, "flow_start_time" TIMESTAMP
, "flow_end_time" TIMESTAMP
, "flow_bytes" BIGINT
, "flow_ms" BIGINT
, "sub_flow_id" VARCHAR(16)
, "cell_id" VARCHAR(8)
, "sub_flow_start_time" TIMESTAMP NOT NULL
, "sub_flow_end_time" TIMESTAMP NOT NULL
, "dummy" INTEGER
);

create or replace pump sub_flow_pump stopped 
as
insert into sub_flows
select stream "user_id"
    ,"flow_id","flow_start_time","flow_end_time","flow_bytes","flow_ms"
    , "sub_flow_id", "cell_id"
    , case 
      when "cell_arrival_time" < "flow_start_time" then "flow_start_time"
      else "cell_arrival_time" 
      end as "sub_flow_start_time"
    , "cell_departure_time" as "sub_flow_end_time"
    , 1 as "dummy"
from flow_cell_3
where "cell_arrival_time" is not null;


create or replace view flow_intervals_1 AS
select stream f.*
     , s."rowno"
     , step("sub_flow_start_time" + (s."rowno" * interval '30' minute) by interval '30' minute) as "interval_start_time"
     , step("sub_flow_start_time" + ((s."rowno" + 1) * interval '30' minute) by interval '30' minute) as "interval_end_time"
from sub_flows f
JOIN flowsplitter s using ("dummy");

create or replace view flow_intervals_2 AS
select stream "user_id","cell_id"
    ,"flow_id","flow_start_time","flow_end_time","flow_bytes","flow_ms"
    ,"sub_flow_id"||'-'||cast("rowno" as varchar(8)) as "flow_interval"
    , case when "interval_start_time" < "sub_flow_start_time" then "sub_flow_start_time" else "interval_start_time" end as "interval_start_time" 
    , case when "interval_end_time" > "sub_flow_end_time" then "sub_flow_end_time" else "interval_end_time" end as "interval_end_time" 
from flow_intervals_1
where period("interval_start_time","interval_end_time") overlaps period("sub_flow_start_time", "sub_flow_end_time")
and "interval_start_time" < "interval_end_time"
;

create or replace view flow_intervals_3 AS
select stream *
            , tsdiff("interval_end_time","interval_start_time") as "interval_ms"
from flow_intervals_2
;

-- NOTE that this order emits ALL the rows for each flow at flow_end_time = rowtime
-- we don't attempt to re-order by interval_start_time because that could introduce several hours of latency

create or replace view flow_intervals_4 AS
select stream *
    , CAST(CAST("flow_bytes" as DOUBLE) * CAST("interval_ms" AS DOUBLE)/CAST("flow_ms" AS DOUBLE) AS BIGINT) as "interval_bytes"
from flow_intervals_3 s
order by s.rowtime, "flow_id", "interval_start_time"
;


!outputformat csv

select stream rowtime 
    ,"flow_id","flow_start_time", "flow_end_time","flow_bytes"
    ,"user_id","cell_id"
    ,"flow_interval","interval_start_time","interval_end_time","interval_bytes"
    ,sum("interval_bytes") over w as "cumulative_bytes"
from flow_intervals_4
window w as (partition by "flow_id" range interval '0' second preceding);

