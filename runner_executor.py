#!/usr/bin/env python2.7
import argparse
import base64
import sys
import zlib
import json
import threading
import subprocess
import copy
import time
import os
from runner_core.runner import run,run2,run2_asan

#from pymesos import MesosExecutorDriver, Executor, decode_data, encode_data
import mesos.interface
from mesos.interface import mesos_pb2
from mesos.executor import MesosExecutorDriver

class Offline(object):
    def __init__(self, cv):
        self.cv = cv

    def sendStatusUpdate(self, update):

        if update.state == 'TASK_FINISHED':
            self.cv.acquire()
            self.cv.notify()
            self.cv.release()

        return

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
            programs = []
            argslist = []
            finished_tasks = []
            write_out = False
            run_these = []
            for task_data in task_data_list:
                program = task_data['program']
                args = task_data['program_args']
                indata = task_data['input_filename']
                is_stdin = task_data['stdin']
                do_read = task_data['do_read']
                task_id = task_data['taskid']

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
                outdata['taskid'] = task_id
                outdata['stack'] = ""

                programs.append(program)
                argslist.append((args, stdindata)) 
                finished_tasks.append(outdata)
           
            runthese = zip(programs,argslist,finished_tasks)
            if len(runthese) > 0:
                try:
                    results = run2_asan(runthese)
                except:
                    update = mesos_pb2.TaskStatus()
                    update.task_id.value = task.task_id.value
                    update.state = mesos_pb2.TASK_FAILED
                    driver.sendStatusUpdate(update)
                    return

            else:
                results = []

            update = mesos_pb2.TaskStatus()
            update.task_id.value = task.task_id.value
            update.state = mesos_pb2.TASK_FINISHED
            update.data = json.dumps(results)
            driver.sendStatusUpdate(update)

        thread = threading.Thread(target=run_task,args=(d,t))
        thread.start()

    def frameworkMessage(self, driver, message):
        # Send it back to the scheduler.
        driver.sendFrameworkMessage(message)

if __name__ == "__main__":
    parser = argparse.ArgumentParser('runner_executor')
    parser.add_argument('--offline', action="store_true", default=False)
    parser.add_argument('--offline-file', type=str, default="")
    args = parser.parse_args()
    if args.offline:
        # Fake out being online by making a dict. 
        cv = threading.Condition() 
        tce = TestCaseExecutor()
        o = Offline(cv)
        with open(args.offline_file, 'r') as inf:
            for line in inf.readlines():
                task = Dict()
                task.data = zlib.decompress(base64.b64decode(line[:-1]))
                tce.launchTask(o, task)
                cv.acquire()
                cv.wait()
                cv.release()
    else:
        driver = MesosExecutorDriver(TestCaseExecutor())
        sys.exit(0 if driver.run() == mesos_pb2.DRIVER_STOPPED else 1)
