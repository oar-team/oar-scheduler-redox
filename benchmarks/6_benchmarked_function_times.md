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

## CoreOnly 64 in switch
Function schedule_job                        called   1000 times, took   460ms ( 460µs on average)
Function find_node_for_moldable_rec          called 230570 times, took 30.89s  ( 133µs on average)
Function fit_state                           called 230570 times, took   415ms (   1µs on average)
Function request                             called 251310 times, took   378ms (   1µs on average)
Function fit_state_in_intersection           called 127994 times, took   131ms (   1µs on average)
Function find_resource_hierarchies_scattered called 643333 times, took   544ms (   0µs on average)
Function sub_proc_set_with_cores             called 392023 times, took   187ms (   0µs on average)


## CoreOnly 256
Function schedule_job                        called   1000 times, took   1.66s  (   1ms on average)
Function find_node_for_moldable_rec          called 446658 times, took 274.41s  ( 614µs on average)
Function fit_state                           called 446658 times, took   1.58s  (   3µs on average)
Function request                             called 545337 times, took   1.50s  (   2µs on average)
Function find_resource_hierarchies_scattered called 545337 times, took   1.38s  (   2µs on average)
Function sub_proc_set_with_cores             called 545337 times, took   1.33s  (   2µs on average)
Function fit_state_in_intersection           called 277049 times, took    120ms (   0µs on average)

## NodeOnly
Function schedule_job                        called   1000 times, took   1s  ( 1047µs on average)
Function find_node_for_moldable_rec          called 436944 times, took 179s  (  409µs on average)
Function fit_state                           called 436944 times, took 965ms ( 2209ns on average)
Function request                             called 537598 times, took 886ms ( 1649ns on average)
Function find_resource_hierarchies_scattered called 537598 times, took 768ms ( 1430ns on average)
Function fit_state_in_intersection           called 274821 times, took 299ms ( 1089ns on average)

## Normal
Function schedule_job                        called    1000 times, took   990ms ( 990µs on average)
Function find_node_for_moldable_rec          called  133567 times, took 15.04s  ( 112µs on average)
Function fit_state                           called  133567 times, took   960ms (   7µs on average)
Function request                             called  156206 times, took   936ms (   5µs on average)
Function fit_state_in_intersection           called   79183 times, took   465ms (   5µs on average)
Function find_resource_hierarchies_scattered called 1828818 times, took  1.53s  (   0µs on average)
Function sub_proc_set_with_cores             called 1480262 times, took    25ms (   0µs on average)

# Execution times for Basic algorithm without cache

## NodeOnly
Function schedule_job                        called   1000 times, took 512ms ( 512µs on average)
Function find_slots_for_moldable             called   1000 times, took 511ms ( 511µs on average)
Function request                             called 354352 times, took 320ms ( 906ns on average)
Function find_resource_hierarchies_scattered called 354352 times, took 259ms ( 732ns on average)
Function intersect_slots_intervals           called 354352 times, took 113ms ( 319ns on average)

# For 100 Jobs - Basic
## NodeOnly
Function schedule_job                        called  100 times, took  6525µs (  65µs on average)
Function find_slots_for_moldable             called  100 times, took  6454µs (  64µs on average)
Function request                             called 3735 times, took  4121µs (1103ns on average)
Function find_resource_hierarchies_scattered called 3735 times, took  3408µs ( 913ns on average)
Function intersect_slots_intervals           called 3735 times, took  1456µs ( 390ns on average)

# For 100 Jobs - Tree
## NodeOnly
Function schedule_job                        called  100 times, took    12ms ( 120µs on average)
Function find_node_for_moldable_rec          called 4396 times, took   208ms (  47µs on average)
Function fit_state                           called 4396 times, took    11ms (2545ns on average)
Function request                             called 5512 times, took    10ms (1889ns on average)
Function find_resource_hierarchies_scattered called 5512 times, took  9245µs (1677ns on average)
Function fit_state_in_intersection           called 2860 times, took  4671µs (1633ns on average)

# For 10 Jobs - Basic
## NodeOnly
Function schedule_job                        called 10 times, took  130µs (  13µs on average)
Function find_slots_for_moldable             called 10 times, took  122µs (  12µs on average)
Function request                             called 47 times, took   86µs (1831ns on average)
Function find_resource_hierarchies_scattered called 47 times, took   76µs (1622ns on average)
Function intersect_slots_intervals           called 47 times, took   20µs ( 435ns on average)

# For 10 Jobs - Tree
## NodeOnly
Function schedule_job                        called 10 times, took  184µs (  18µs on average)
Function find_node_for_moldable_rec          called 47 times, took  514µs (  10µs on average)
Function fit_state                           called 47 times, took  166µs (3532ns on average)
Function fit_state_in_intersection           called 35 times, took   98µs (2820ns on average)
Function request                             called 60 times, took  156µs (2604ns on average)
Function find_resource_hierarchies_scattered called 60 times, took  130µs (2172ns on average)

## NodeOnly - intersection first
Function schedule_job                        called  10 times, took   367µs (  36µs on average)
Function find_node_for_moldable_rec          called 100 times, took  1408µs (  14µs on average)
Function fit_state                           called 100 times, took   344µs (3446ns on average)
Function request                             called 115 times, took   325µs (2830ns on average)
Function fit_state_in_intersection           called  70 times, took   191µs (2735ns on average)
Function find_resource_hierarchies_scattered called 115 times, took   287µs (2497ns on average)


