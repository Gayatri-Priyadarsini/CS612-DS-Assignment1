# CS612-DS-Assignment1

This Assignment required us to edit three files:
1. coordinator.go
2. rpc.go
3. worker.go

# coordinator.go:
Has routines to maintain the status of Map and Reduce Jobs
Tasks to be done:
1. Data structure to hold MR status
2. Communication between Coordinator and worker
3. Handle M/R job in worker and produce proper opt
4. Coordinate between mappers and reducers

# rpc.go:
Has definition of different types of calls between workers and coordinator and their arguments
Tasks to be Done:
1. Reuest a Job (Map,Reduce)
2. Report a Job finished (Input file, Intermediate file, Reducers)


# worker.go:
Has routines to execute map/reduce job
Tasks to be done:
1. Map
2.Reduce

