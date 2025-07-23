# Execution times of the benchmarked functions
## Basic (no cache) NodeOnly 1000 Jobs
1000 of 1000 jobs scheduled in 4108 ms (39% cache hits, 877 slots, 6920/6477h width (93% usage), 0% quotas hit)
Function schedule_job                        called   1000 times, took 4104ms ( 4104µs on average)
Function find_slots_for_moldable             called   1000 times, took 4100ms ( 4100µs on average)
Function request                             called 369698 times, took 2887ms ( 7810ns on average)
Function find_resource_hierarchies_scattered called 369698 times, took 2515ms ( 6803ns on average)
Function intersect_slots_intervals           called 369698 times, took  816ms ( 2210ns on average)

## Tree NodeOnly 1000 Jobs
1000 of 1000 jobs scheduled in 9209 ms (39% cache hits, 821 slots, 7019/6491h width (92% usage), 0% quotas hit)
Function schedule_job                        called   1000 times, took 9134ms ( 9134µs on average)
Function find_node_for_moldable_rec          called 446703 times, took 1509s  ( 3377µs on average)
Function fit_state                           called 446703 times, took 8688ms (   19µs on average)
Function request                             called 555886 times, took 8373ms (   15µs on average)
Function find_resource_hierarchies_scattered called 555886 times, took 7756ms (   13µs on average)
Function fit_state_in_intersection           called 287059 times, took 1719ms ( 5989ns on average)


## Basic (no cache) NodeOnly 7 Jobs
0: 0->539 (proc_set 1..=7936)
1: 540->1259 (proc_set 1..=2816)
2: 540->1019 (proc_set 2817..=6656)
3: 1020->1139 (proc_set 2817..=8704)
4: 1140->2519 (proc_set 2817..=9472)  VS 1260->2639 (proc_set 1..=6656)
5: 2520->3659 (proc_set 1..=4096)
6: 2520->3899 (proc_set 4097..=9728)
7 of 7 jobs scheduled in 0 ms (0% cache hits, 8 slots, 64/50h width (78% usage), 0% quotas hit)
Function schedule_job                        called  7 times, took  778µs ( 111µs on average)
Function find_slots_for_moldable             called  7 times, took  716µs ( 102µs on average)
Function request                             called 24 times, took  571µs (  23µs on average)
Function find_resource_hierarchies_scattered called 24 times, took  539µs (  22µs on average)
Function intersect_slots_intervals           called 24 times, took   76µs (3184ns on average)

## Tree NodeOnly 7 Jobs
0: 0->539 (proc_set 1..=7936)
1: 540->1259 (proc_set 1..=2816)
2: 540->1019 (proc_set 2817..=6656)
3: 1020->1139 (proc_set 2817..=8704)
4: 1260->2639 (proc_set 1..=6656)
5: 2640->3779 (proc_set 1..=4096)
6: 2640->4019 (proc_set 4097..=9728)
7 of 7 jobs scheduled in 2 ms (0% cache hits, 8 slots, 66/50h width (75% usage), 0% quotas hit)
Function schedule_job                        called  7 times, took  1744µs ( 249µs on average)
Function find_node_for_moldable_rec          called 33 times, took  3578µs ( 108µs on average)
Function fit_state                           called 33 times, took  1544µs (  46µs on average)
Function fit_state_in_intersection           called 26 times, took  1004µs (  38µs on average)
Function request                             called 41 times, took  1502µs (  36µs on average)
Function find_resource_hierarchies_scattered called 41 times, took  1413µs (  34µs on average)
