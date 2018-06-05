#!/usr/bin/env python2.7

import sys
import json
import threading
from runner_core.runner import run

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
        # Create a thread to run the task. Tasks should always be run in new
        # threads or processes, rather than inside launchTask itself.
        def run_task(driver,task):
            update = mesos_pb2.TaskStatus()
            update.task_id.value = task.task_id.value
            update.state = mesos_pb2.TASK_RUNNING
            driver.sendStatusUpdate(update)

            # This is where one would perform the requested task.
            task_data = json.loads(task.data)
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
                args.append(indata)

            xmldata = run(program, args, stdindata)

            update = mesos_pb2.TaskStatus()
            update.task_id.value = task.task_id.value
            update.state = mesos_pb2.TASK_FINISHED
            update.data = xmldata
            driver.sendStatusUpdate(update)

        thread = threading.Thread(target=run_task,args=(d,t))
        thread.start()

    def frameworkMessage(self, driver, message):
        # Send it back to the scheduler.
        driver.sendFrameworkMessage(message)

if __name__ == "__main__":
    driver = MesosExecutorDriver(TestCaseExecutor())
    sys.exit(0 if driver.run() == mesos_pb2.DRIVER_STOPPED else 1)
