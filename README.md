# UCP

Given Control Plane and flow data as below, give the output

	
## Control Plane
The latest cell a user is in

```
event_end_time, user_id, cell_id
2021-06-21 04:00, user_1, cell_1
2021-06-21 05:30, user_1, cell_2
2021-06-21 06:30, user_1, cell_1
```

## Flow
```
event_end_time, event_start_time, flow_id, user_id, bytes
2021-06-21 05:15, 2021-06-21 04:15, flow_1, user_1, 60
2021-06-21 06:40, 2021-06-21 04:20, flow_2, user_1, 140
```

## Output
* For each flow, match to the corresponding cell locations.
* Break at 15 minute intervals even when cell does not move
* Pro-rate the flow traffic across all intervals for that flow

```
end_time, start_time, flow_id, cell_id, user_id, bytes
2021-06-21 04:30, 2021-06-21 04:15, flow_1, cell_1, user_1, 15
2021-06-21 05:00, 2021-06-21 04:30, flow_1, cell_1, user_1, 30
2021-06-21 05:15, 2021-06-21 05:00, flow_1, cell_1, user_1, 15
2021-06-21 04:30, 2021-06-21 04:20, flow_2, cell_1, user_1, 10
2021-06-21 05:00, 2021-06-21 04:30, flow_2, cell_1, user_1, 30
2021-06-21 05:30, 2021-06-21 05:00, flow_2, cell_1, user_1, 30
2021-06-21 06:00, 2021-06-21 05:30, flow_2, cell_2, user_1, 30
2021-06-21 06:30, 2021-06-21 06:00, flow_2, cell_2, user_1, 30
2021-06-21 06:40, 2021-06-21 06:30, flow_2, cell_1, user_1, 10
```
