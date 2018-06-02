# Apache Mesos fuzz test case runner

Some code to run crashing inputs against different versions of 
programs and coallate the crashing / not crashing results. There are
several sub-components: 

1. `runner_framework`: This implements the Mesos Framework API, and is the
   "core logic" of the system. As cluster resources become available, it 
   starts executors on those resources, then feeds those resources jobs until 
   there are no more jobs. 
2. `runner_executor`: This implements the Mesos Executor API, and runs on each
   compute node. As a single threaded task, it accepts new jobs from the 
   framework and dispatches them, gathering their results and posting them to
   the central database.
3. `runner_core`: This is a library that runs a single task. That task, for 
   the moment, is parameterized by a program, a command line, and an input type, 
   either stdin or file. The program is run under `valgrind` and the XML output
   from `valgrind` is parsed into a report. 
4. `report_collector`: A web API that accepts posts from `runner_executor` when
   a batch of work is done. Stores the results into a database. 

## Database structure 

The columns will look something like this:

1. Task ID (unique key)
2. Task run time 
3. Boolean - did the task crash or not
4. String - what is the call stack of the crash, if it exists? 

## Authentication and access

All authentication and access will be done over ssh. To access 
`report_collector`, each node will have access to a local port that is an 
ssh forward back to the collector. That is manageable. 
