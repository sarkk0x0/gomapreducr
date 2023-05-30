# gomapreducr
## MapReduce library

This is an implementation of mapreduce. Lab 1 from MIT 6.824 course.

Whitepaper: http://nil.csail.mit.edu/6.824/2022/papers/mapreduce.pdf

### Notes
1. Unlike the whitepaper implementation, workers pull tasks from coordinator.
2. Single-node implementation (for now)
3. Worker <-> Coordinator communication via RPC sockets
4. Map and Reduce functions loaded via go plugins