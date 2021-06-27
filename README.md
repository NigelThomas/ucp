# UCP

Given Control Plane and flow data as below, give the output as below.

## Implementation Assumptions
* There must always be at least one cell position before the first flow
* Latest cell position will always be within 24 hours
  * see join window in `flow_cell_1` to increase
* Max number of cell positions during a flow = 20
  * see `cellsplitter` and `cp_step_2` to increase
* Max length of flow = 4 hours (8 * 30 minute interval
  * see `flowsplitter` and `flow_intervals_1`
  * Increase by adding rows to `flowsplitter` view.


	
## Control Plane
The latest cell a user is in.


```
arrival_time,user_id,cell_id
2021-06-21 03:05:00,user_2,cell_4
2021-06-21 03:30:00,user_3,cell_3
etc
```
See `cp.csv`

## Flow
Flow start and end times wth throughput in bytes. 

This data is already sessionized, and arrives at flow_end_time.

```
flow_start_time,flow_end_time,flow_id,user_id,bytes
2021-06-21 04:15:00,2021-06-21 05:15:00,flow_1,user_1,60
2021-06-21 04:20:00,2021-06-21 06:40:00,flow_2,user_1,140
etc
```

See `flow.csv`

## Output
* For each flow, match to the corresponding cell locations.
* Break at 30 minute intervals even when cell does not move
* Pro-rate the flow traffic across all intervals for that flow

```
select stream rowtime 
,"flow_id","flow_start_time", "flow_end_time","flow_bytes" 
,"user_id","cell_id" 
,"flow_interval","interval_start_time","interval_end_time","interval_bytes" 
,sum("interval_bytes") over w as "cumulative_bytes" 
from flow_intervals_4 
window w as (partition by "flow_id" range interval '0' second preceding);
'ROWTIME','flow_id','flow_start_time','flow_end_time','flow_bytes','user_id','cell_id','flow_interval','interval_start_time','interval_end_time','interval_bytes','cumulative_bytes'
'2021-06-21 05:15:00.001','flow_1','2021-06-21 04:15:00.0','2021-06-21 05:15:00.0','60','user_1','cell_1','flow_1-3-0','2021-06-21 04:15:00.0','2021-06-21 04:18:00.0','3','3'
'2021-06-21 05:15:00.001','flow_1','2021-06-21 04:15:00.0','2021-06-21 05:15:00.0','60','user_1','cell_2','flow_1-2-0','2021-06-21 04:18:00.0','2021-06-21 04:25:00.0','7','10'
'2021-06-21 05:15:00.001','flow_1','2021-06-21 04:15:00.0','2021-06-21 05:15:00.0','60','user_1','cell_3','flow_1-1-0','2021-06-21 04:25:00.0','2021-06-21 04:30:00.0','5','15'
'2021-06-21 05:15:00.001','flow_1','2021-06-21 04:15:00.0','2021-06-21 05:15:00.0','60','user_1','cell_3','flow_1-1-1','2021-06-21 04:30:00.0','2021-06-21 05:00:00.0','30','45'
'2021-06-21 05:15:00.001','flow_1','2021-06-21 04:15:00.0','2021-06-21 05:15:00.0','60','user_1','cell_3','flow_1-1-2','2021-06-21 05:00:00.0','2021-06-21 05:05:00.0','5','50'
'2021-06-21 05:15:00.001','flow_1','2021-06-21 04:15:00.0','2021-06-21 05:15:00.0','60','user_1','cell_1','flow_1-0-0','2021-06-21 05:05:00.0','2021-06-21 05:15:00.0','10','60'
'2021-06-21 06:40:00.001','flow_2','2021-06-21 04:20:00.0','2021-06-21 06:40:00.0','140','user_1','cell_2','flow_2-4-0','2021-06-21 04:20:00.0','2021-06-21 04:25:00.0','5','5'
'2021-06-21 06:40:00.001','flow_2','2021-06-21 04:20:00.0','2021-06-21 06:40:00.0','140','user_1','cell_3','flow_2-3-0','2021-06-21 04:25:00.0','2021-06-21 04:30:00.0','5','10'
'2021-06-21 06:40:00.001','flow_2','2021-06-21 04:20:00.0','2021-06-21 06:40:00.0','140','user_1','cell_3','flow_2-3-1','2021-06-21 04:30:00.0','2021-06-21 05:00:00.0','30','40'
'2021-06-21 06:40:00.001','flow_2','2021-06-21 04:20:00.0','2021-06-21 06:40:00.0','140','user_1','cell_3','flow_2-3-2','2021-06-21 05:00:00.0','2021-06-21 05:05:00.0','5','45'
'2021-06-21 06:40:00.001','flow_2','2021-06-21 04:20:00.0','2021-06-21 06:40:00.0','140','user_1','cell_1','flow_2-2-0','2021-06-21 05:05:00.0','2021-06-21 05:30:00.0','25','70'
'2021-06-21 06:40:00.001','flow_2','2021-06-21 04:20:00.0','2021-06-21 06:40:00.0','140','user_1','cell_2','flow_2-1-0','2021-06-21 05:30:00.0','2021-06-21 06:00:00.0','30','100'
'2021-06-21 06:40:00.001','flow_2','2021-06-21 04:20:00.0','2021-06-21 06:40:00.0','140','user_1','cell_2','flow_2-1-1','2021-06-21 06:00:00.0','2021-06-21 06:30:00.0','30','130'
'2021-06-21 06:40:00.001','flow_2','2021-06-21 04:20:00.0','2021-06-21 06:40:00.0','140','user_1','cell_1','flow_2-0-0','2021-06-21 06:30:00.0','2021-06-21 06:40:00.0','10','140'
'2021-06-21 06:50:00.001','flow_3','2021-06-21 06:10:00.0','2021-06-21 06:50:00.0','20','user_2','cell_4','flow_3-1-0','2021-06-21 06:10:00.0','2021-06-21 06:30:00.0','10','10'
'2021-06-21 06:50:00.001','flow_3','2021-06-21 06:10:00.0','2021-06-21 06:50:00.0','20','user_2','cell_4','flow_3-1-1','2021-06-21 06:30:00.0','2021-06-21 06:40:00.0','5','15'
'2021-06-21 06:50:00.001','flow_3','2021-06-21 06:10:00.0','2021-06-21 06:50:00.0','20','user_2','cell_1','flow_3-0-0','2021-06-21 06:40:00.0','2021-06-21 06:50:00.0','5','20'
'2021-06-21 06:50:00.001','flow_4','2021-06-21 06:12:00.0','2021-06-21 06:50:00.0','20','user_3','cell_3','flow_4-0-0','2021-06-21 06:12:00.0','2021-06-21 06:30:00.0','9','9'
'2021-06-21 06:50:00.001','flow_4','2021-06-21 06:12:00.0','2021-06-21 06:50:00.0','20','user_3','cell_3','flow_4-0-1','2021-06-21 06:30:00.0','2021-06-21 06:50:00.0','11','20'
```



