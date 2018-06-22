#!/usr/bin/env python2.7
import argparse 
import getpass
import socket
import signal
import time
import json 
import csv
import sys
import os

from threading import Thread
from runner_core.runner import stack_from_xml
from pymesos import MesosSchedulerDriver, Scheduler, decode_data, encode_data
from addict import Dict

def get_tasks(data_dir):
    """
    proglist format : id,program,args
    inputlist format: filename,stdin
    """
    results = []
    proglist = csv.reader(open('{}/proglist.csv'.format(data_dir), 'r'))
    inputlist = csv.reader(open('{}/inputlist.csv'.format(data_dir), 'r'))
    taskid = 0
    first = True
    inputs = []
    programs = []
    for p in proglist:
        if first == True:
            first = False
        else:
            programs.append(p)
    first = True
    for i in inputlist:
        if first == True:
            first = False
        else:
            inputs.append(i)

    for p in programs:
        program_id = p[0]
        program = p[1]
        if program[0] != os.path.sep:
            program = os.path.abspath("{0}/{1}".format(data_dir,program))

        if len(p[2]) > 0:
            program_args = p[2].split(" ")
        else:
            program_args = []
        for i in inputs:
            input_filename = i[0]
            if input_filename[0] != os.path.sep:
                input_filename = os.path.abspath("{0}/{1}".format(data_dir,input_filename))
            do_read = i[1]
            if do_read == 'False':
                do_read = False
            else:
                do_read = True
            is_stdin = i[2]
            if is_stdin == 'False':
                is_stdin = False
            else:
                is_stdin = True
            results.append((taskid,program,program_args,input_filename,do_read,is_stdin,program_id))
            taskid = taskid + 1
    return results


class TestCaseScheduler(Scheduler):
    def __init__(self, tasks, output, executor, batch):
        self.tasks = tasks
        self.taskNum = len(tasks)
        self.executor = executor
        self.tasksLaunched = 0
        self.taskData = {}
        self.batchSize = batch
        self.results = []
        self.outfile = open(output, 'w')
        self.outwriter = csv.writer(self.outfile)
        self.outwriter.writerow(["program_hash","input_file","result"])

    def _finished(self, state):
        return state == 'TASK_FINISHED'

    def _failed(self, state):
        r = False
        if state == 'TASK_FAILED':
            r = True
        elif state == 'TASK_ERROR':
            r = True
        elif state == 'TASK_KILLED':
            r = True
        return r

    def registered(self, driver, frameworkId, masterInfo):
        print "Registered with framework ID %s" % frameworkId.value

    def getResource(self, res, name):
        for r in res:
            if r.name == name:
                return r.scalar.value
        return 0.0

    def resourceOffers(self, driver, offers):
        for offer in offers:
            offerCpus = self.getResource(offer.resources, 'cpus')
            offerMem = self.getResource(offer.resources, 'mem')
            
            remainingCpus = offerCpus
            remainingMem = offerMem
            assigned_tasks = []

            print "Received offer %s with cpus: %s and mem: %s" \
                  % (offer.id.value, offerCpus, offerMem)
            
            while len(self.tasks) > 0 and remainingCpus >= 1 and remainingMem >= 512:
                # Schedule a task. 
                tid = self.tasksLaunched
                self.tasksLaunched += 1

                print "Launching task %d using offer %s" \
                      % (tid, offer.id.value)

                task = Dict()
                task_id = str(tid)  
                task.task_id.value = task_id 
                task.agent_id.value = offer.agent_id.value
                task.name = "task %d" % tid
                task.executor = self.executor

                task_data_list = []
                for i in range(0,self.batchSize):
                    if len(self.tasks) == 0:
                        continue

                    workunit = self.tasks.pop()
                    task_data = {}
                    task_data['taskid'] = workunit[0]
                    task_data['program'] = "file://{}".format(workunit[1])
                    task_data['program_args'] = workunit[2]
                    task_data['input_filename'] = "file://{}".format(workunit[3])
                    task_data['do_read'] = workunit[4]
                    task_data['stdin'] = workunit[5]
                    task_data['program_id'] = workunit[6]
                    task_data_list.append(task_data)
                    #self.taskData[task.task_id.value] = workunit

                if len(task_data_list) == 0:
                    continue

                task.data = encode_data(json.dumps(task_data_list))
                task.resources = [
                    dict(name='cpus', type='SCALAR', scalar={'value': 1}),
                    dict(name='mem', type='SCALAR', scalar={'value': 512}),
                ]
	
                assigned_tasks.append(task)

                # This is how much we used. 
                remainingCpus = remainingCpus - 1
                remainingMem = remainingMem - 512
            
            # Propose to launch the tasks. 
            driver.launchTasks(offer.id, assigned_tasks)

    def statusUpdate(self, driver, update):
        print 'Status update TID %s %s' % (update.task_id.value, update.state)
        # First case, maybe the task is finished? 
        if self._finished(update.state):
            results = json.loads(decode_data(update.data))
            for result in results:
                stack = stack_from_xml(result['stack'])
                if stack == None:
                    stack = "-nocrash-"
                self.outwriter.writerow([result['hash'], result['inputfile'], "-".join(stack)])
                self.results.append(stack)
        
        # If the task was killed or was lost or failed, we should re-queue it.
        if self._failed(update.state):
            print "something failed!"
            print update
            driver.stop()

        # Maybe, we have all the results?
        if len(self.results) == self.taskNum:
            print "Got all the results"
            driver.stop()
        return

    def frameworkMessage(self, driver, executorId, slaveId, message):
        return

def main(args):
    # First, read the data we need to produce the task list. 
    task_list = [] # A list of commands to run of the form (seq,command,args,input,stdin)
    task_list = get_tasks(args.data_dir)
    executor = Dict()
    executor.executor_id.value = 'TestCaseExecutor'
    executor.name = executor.executor_id.value
    executor.command.value = '%s %s' % (
        sys.executable,
        os.path.abspath(os.path.join(os.path.dirname(__file__), 'runner_executor.py'))
    )
    executor.resources = [
        dict(name='mem', type='SCALAR', scalar={'value': 256}),
        dict(name='cpus', type='SCALAR', scalar={'value': 0.1}),
    ]

    framework = Dict()
    framework.user = getpass.getuser()
    framework.name = "TestCaseFramework"
    framework.hostname = socket.gethostname()

    driver = MesosSchedulerDriver(
        TestCaseScheduler(task_list, args.output, executor, args.batch),
        framework,
        args.controller,
        use_addict=True,
    )

    def signal_handler(signal, frame):
        driver.stop()

    def run_driver_thread():
        driver.run()

    driver_thread = Thread(target=run_driver_thread, args=())
    driver_thread.start()

    print('Scheduler running, Ctrl+C to quit.')
    signal.signal(signal.SIGINT, signal_handler)

    while driver_thread.is_alive():
        time.sleep(1)	

	return 0

if __name__ == '__main__':
    parser = argparse.ArgumentParser('framework')
    parser.add_argument('--batch', type=int, default=10, help="Batch size")
    parser.add_argument('data_dir', type=str, help="Data directory")
    parser.add_argument('controller', type=str, help="Controller URI")
    parser.add_argument('output', type=str, help="Output file")
    args = parser.parse_args()
    sys.exit(main(args))
