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
from runner_core.runner import run,run2

#from pymesos import MesosExecutorDriver, Executor, decode_data, encode_data
import mesos.interface
from mesos.interface import mesos_pb2
from mesos.executor import MesosExecutorDriver

from addict import Dict

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
            #update = Dict()
            update = mesos_pb2.TaskStatus()
            update.task_id.value = task.task_id.value
            #update.state = 'TASK_RUNNING'
            update.state = mesos_pb2.TASK_RUNNING
            #update.timestamp = time.time()
            driver.sendStatusUpdate(update)

            #task_data_list = json.loads(decode_data(task.data))
            task_data_list = json.loads(task.data)
            failed_tasks = []
            finished_tasks = []
            program = None
            argslist = []
            finished_tasks = []
            write_out = False
            for task_data in task_data_list:
                if task_data.has_key('to_fs'):
                    write_out = True
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
                a = outdata['inputfile']
                b = outdata['hash']
                if a[:7] == "file://":
                    a = a[7:]
                nm = "{0}-{1}.xml.gz".format(a,b)
                if write_out and not os.path.isfile(nm):
                    argslist.append((args, stdindata)) 
                    finished_tasks.append(outdata)
            
            if len(argslist) > 0:
                results = run2(program, argslist)
            else:
                results = []

            for i in range(0, len(results)):
                finished_tasks[i]['stack'] = results[i]
                if write_out:
                    a = finished_tasks[i]['inputfile']
                    b = finished_tasks[i]['hash']
                    if a[:7] == "file://":
                        a = a[7:]
                    of = open("{0}-{1}.xml".format(a,b), 'w')
                    of.write(results[i])
                    of.close()
                    subprocess.call(['/bin/gzip', "-f", "{0}-{1}.xml".format(a,b)])
             
            #update = Dict()
            update = mesos_pb2.TaskStatus()
            update.task_id.value = task.task_id.value
            #update.state = 'TASK_FINISHED'
            update.state = mesos_pb2.TASK_FINISHED
            #update.state = 'TASK_FINISHED'
            #update.timestamp = time.time()
            #update.data = encode_data(json.dumps(finished_tasks))
            update.data = json.dumps(finished_tasks)
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
        #driver = MesosExecutorDriver(TestCaseExecutor(), use_addict=True)
        #driver.run()
