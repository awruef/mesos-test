#!/usr/bin/env python2.7

import sys
import json
import threading
import copy
from runner_core.runner import run,run2

import mesos.interface
from mesos.interface import mesos_pb2
from mesos.executor import MesosExecutorDriver

class TestCaseExecutor(mesos.interface.Executor):
    """
    If this becomes a 1:N mapping for a queue of test cases, then 
    we should add some logic that will surrender tasks when they are 
    done. I don't know if we need that, though. 
    """
    def launchTask(self, d, t):
        def run_task(driver,task):
            # Tell everyone we've picked up and are running the task. 
            update = mesos_pb2.TaskStatus()
            update.task_id.value = task.task_id.value
            update.state = mesos_pb2.TASK_RUNNING
            driver.sendStatusUpdate(update)

            task_data_list = json.loads(task.data)
            failed_tasks = []
            finished_tasks = []
            program = None
            argslist = []
            finished_tasks = []
            for task_data in task_data_list:
                program = task_data['program']
                args = task_data['program_args']
                indata = task_data['input_filename']
                is_stdin = task_data['stdin']
                do_read = task_data['do_read']

                if do_read:
                    indata = open(indata, 'r').read()

                stdindata = None
                if is_stdin:
                    stdindata = indata
                else:
                    if len(args) > 0:
                        args.append(indata)
                    else:
                        args = [indata]

                outdata = {}
                outdata['hash'] = task_data['program_id']
                outdata['inputfile'] = task_data['input_filename']
                outdata['stack'] = ""
                argslist.append((args, stdindata)) 
                finished_tasks.append(outdata)

            results = run2(program, argslist)

            for i in range(0, len(results)):
                finished_tasks[i]['stack'] = results[i]
             
            update = mesos_pb2.TaskStatus()
            update.task_id.value = task.task_id.value
            update.state = mesos_pb2.TASK_FINISHED
            update.data = json.dumps(finished_tasks)
            driver.sendStatusUpdate(update)

        thread = threading.Thread(target=run_task,args=(d,t))
        thread.start()

    def frameworkMessage(self, driver, message):
        # Send it back to the scheduler.
        driver.sendFrameworkMessage(message)

if __name__ == "__main__":
    driver = MesosExecutorDriver(TestCaseExecutor())
    sys.exit(0 if driver.run() == mesos_pb2.DRIVER_STOPPED else 1)
