# Execution times for Tree algorithm functions
## OldNormal
Function schedule_job                        called   1000 times, took  174ms ( 174µs on average)
Function find_node_for_moldable_rec          called 158943 times, took 7.79s  (  49µs on average)
Function fit_state                           called 158943 times, took  131ms (   0µs on average)
Function request                             called 161770 times, took  107ms (   0µs on average)
Function fit_state_in_intersection           called  80674 times, took   34ms (   0µs on average)
Function find_resource_hierarchies_scattered called 161770 times, took   69ms (   0µs on average)
Function sub_proc_set_with_cores             called 161770 times, took   54ms (   0µs on average)

## CoreOnly 64
Function schedule_job                        called   1000 times, took   331ms ( 331µs on average)
Function find_node_for_moldable_rec          called 223198 times, took 21.17s  (  94µs on average)
Function fit_state                           called 223198 times, took   288ms (   1µs on average)
Function request                             called 242378 times, took   252ms (   1µs on average)
Function find_resource_hierarchies_scattered called 242378 times, took   199ms (   0µs on average)
Function sub_proc_set_with_cores             called 242378 times, took   175ms (   0µs on average)
Function fit_state_in_intersection           called 125379 times, took    53ms (   0µs on average)

## CoreOnly 256
Function schedule_job                        called   1000 times, took   1.66s  (   1ms on average)
Function find_node_for_moldable_rec          called 446658 times, took 274.41s  ( 614µs on average)
Function fit_state                           called 446658 times, took   1.58s  (   3µs on average)
Function request                             called 545337 times, took   1.50s  (   2µs on average)
Function find_resource_hierarchies_scattered called 545337 times, took   1.38s  (   2µs on average)
Function sub_proc_set_with_cores             called 545337 times, took   1.33s  (   2µs on average)
Function fit_state_in_intersection           called 277049 times, took    120ms (   0µs on average)

## NodeOnly
Function schedule_job                        called   1000 times, took   1.09s  (   1ms on average)
Function find_node_for_moldable_rec          called 430440 times, took 181.27s  ( 421µs on average)
Function fit_state                           called 430440 times, took   1.01s  (   2µs on average)
Function request                             called 536209 times, took    935ms (   1µs on average)
Function find_resource_hierarchies_scattered called 536209 times, took    818ms (   1µs on average)
Function fit_state_in_intersection           called 271948 times, took    318ms (   1µs on average)
