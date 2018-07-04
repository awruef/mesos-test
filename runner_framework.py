#!/usr/bin/env python2.7
import argparse 
import zlib
import base64
import getpass
import socket
import signal
import time
import json 
import copy
import csv
import sys
import os

from threading import Thread, Lock
from runner_core.runner import stack_from_xml
#from pymesos import MesosSchedulerDriver, Scheduler, decode_data, encode_data
import mesos.interface
from mesos.interface import mesos_pb2
from mesos.scheduler import MesosSchedulerDriver
from addict import Dict

def get_tasks_size(data_dir):
    proglist = csv.reader(open('{}/proglist.csv'.format(data_dir), 'r'))
    inputlist = csv.reader(open('{}/inputlist.csv'.format(data_dir), 'r'))
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
    
    return len(inputs)*len(programs)

def get_tasks(data_dir, program_idx):
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

    if program_idx >= len(programs):
        return []

    p = programs[program_idx]
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

def make_task_data(workunit):
    task_data = {}
    task_data['taskid'] = workunit[0]
    task_data['program'] = "file://{}".format(workunit[1])
    task_data['program_args'] = workunit[2]
    task_data['input_filename'] = "file://{}".format(workunit[3])
    task_data['do_read'] = workunit[4]
    task_data['stdin'] = workunit[5]
    task_data['program_id'] = workunit[6]
    return task_data 

class TestCaseScheduler(mesos.interface.Scheduler):
    def __init__(self, data_dir, output, executor, batch):
        self.tasks = []
        self.taskMax = get_tasks_size(data_dir)
        self.data_dir = data_dir
        self.cur_program_idx = 0
        self.lock = Lock()
        self.executor = executor
        self.tasksLaunched = 0
        self.taskData = {}
        self.batchSize = batch
        self.results = []
        self.outfile = open(output, 'w')
        self.outwriter = csv.writer(self.outfile)
        self.outwriter.writerow(["program_hash","input_file","result"])

    def _finished(self, state):
        return state == mesos_pb2.TASK_FINISHED

    def _failed(self, state):
        r = False
        if state == mesos_pb2.TASK_FAILED:
            r = True
        elif state == mesos_pb2.TASK_ERROR:
            r = True
        elif state == mesos_pb2.TASK_KILLED:
            r = True
        elif state == mesos_pb2.TASK_LOST:
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
        self.lock.acquire() 
        if len(self.tasks) == 0:
            # Try to get a new set of tasks to do. 
            newTasks = get_tasks(self.data_dir, self.cur_program_idx)
            if len(newTasks) > 0:
                self.tasks = newTasks
                self.cur_program_idx = self.cur_program_idx + 1
        
        print "Progress: %d/%d" % (len(self.results),self.taskMax)
        for offer in offers:
            #offerCpus = self.getResource(offer.resources, 'cpus')
            #offerMem = self.getResource(offer.resources, 'mem')
            offerCpus = 0
            offerMem = 0
            for resource in offer.resources:
                if resource.name == "cpus":
                    offerCpus += resource.scalar.value
                elif resource.name == "mem":
                    offerMem += resource.scalar.value
            
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
                task = mesos_pb2.TaskInfo()
                cpus = task.resources.add()
                mem = task.resources.add()
                
                task.task_id.value = str(tid)
                task.slave_id.value = offer.slave_id.value
                #task = Dict()
                #task_id = str(tid)  
                #task.task_id.value = task_id 
                #task.agent_id.value = offer.agent_id.value
                task.name = "task %d" % tid
                #task.executor = self.executor

                task_data_list = []
                units = []
                for i in range(0,self.batchSize):
                    if len(self.tasks) == 0:
                        continue

                    workunit = self.tasks.pop()
                    units.append(workunit)
                    task_data = make_task_data(workunit)
                    task_data_list.append(task_data)

                if len(task_data_list) == 0:
                    continue

                #task.data = encode_data(json.dumps(task_data_list))
                task.data = json.dumps(task_data_list)
                task.executor.MergeFrom(self.executor)
                cpus.name = "cpus"
                cpus.type = mesos_pb2.Value.SCALAR
                cpus.scalar.value = 1
                mem.name = "mem"
                mem.type = mesos_pb2.Value.SCALAR
                mem.scalar.value = 512
                #task.resources = [
                #    dict(name='cpus', type='SCALAR', scalar={'value': 1}),
                #    dict(name='mem', type='SCALAR', scalar={'value': 512}),
                #]
	
                assigned_tasks.append(task)
                self.taskData[task.task_id.value] = copy.deepcopy(units)

                # This is how much we used. 
                remainingCpus = remainingCpus - 1
                remainingMem = remainingMem - 512
            
            # Propose to launch the tasks. 
            operation = mesos_pb2.Offer.Operation()
            operation.type = mesos_pb2.Offer.Operation.LAUNCH
            operation.launch.task_infos.extend(assigned_tasks)
            driver.acceptOffers([offer.id], [operation])            
            #driver.launchTasks(offer.id, assigned_tasks)
        self.lock.release()

    def statusUpdate(s, d, u):
        def worker(self,driver,update):
            print 'Status update TID %s %s' % (update.task_id.value, update.state)
            # First case, maybe the task is finished? 
            if self._finished(update.state):
                #results = json.loads(decode_data(update.data))
                results = json.loads(update.data)
                for result in results:
                    self.lock.acquire()
                    writedata = base64.b64encode(result['stack'])
                    self.outwriter.writerow([result['hash'], result['inputfile'], writedata])
                    self.results.append(update.task_id.value)
                    self.lock.release()
        
            # If the task was killed or was lost or failed, we should re-queue it.
            if self._failed(update.state):
                units = self.taskData[update.task_id.value]
                print "something failed!"
                print update
                self.lock.acquire()
                self.tasks.extend(units)
                self.lock.release()

            # Maybe, we have all the results?
            self.lock.acquire()
            if len(self.results) == self.taskMax:
                print "Got all the results"
                driver.stop()
            self.lock.release() 
            return

        thread = Thread(target=worker,args=(s,d,u))
        thread.start()
        return

    def frameworkMessage(self, driver, executorId, slaveId, message):
        return

def main(args):
    if args.offline:
        # Get all the tasks. 
        i = 0
        l = []
        while True:
            newTasks = get_tasks(args.data_dir, i)
            if len(newTasks) == 0:
                break
            l.extend(newTasks)
            i = i + 1

        # Create JSON blobs for each task, including batch size.
        of = open(args.output, 'w')
        while len(l) > 0:
            task_data_list = []
            for i in range(0,args.batch):
                if len(l) == 0:
                    continue

                workunit = l.pop()
                task_data = make_task_data(workunit)
                task_data['to_fs'] = 0
                task_data_list.append(task_data)

            if len(task_data_list) == 0:
                continue

            data = encode_data(json.dumps(task_data_list))
	
            # Write the JSON blob to the output file, one per line. 
            of.write("{}\n".format(base64.b64encode(zlib.compress(data))))

        of.close()
        return 0
    # Get the path to our executor
    executor_path = os.path.abspath("./executor")
    # Then, boot up all the Mesos crap and get us registered with the framework. 

    executor = mesos_pb2.ExecutorInfo()
    executor.executor_id.value = "test-case-executor"
    executor.command.value = executor_path
    executor.name = "Test case repeater"
    cpus = executor.resources.add()
    mem = executor.resources.add()
    cpus.name = "cpus"
    cpus.type = mesos_pb2.Value.SCALAR
    cpus.scalar.value = 0
    mem.name = "mem"
    mem.type = mesos_pb2.Value.SCALAR
    mem.scalar.value = 128
 
    #executor.container.MergeFrom(container)

    framework = mesos_pb2.FrameworkInfo()
    framework.user = "" # Have Mesos fill in the current user.
    framework.name = "Test case repeater framework"
    framework.checkpoint = True
    framework.principal = "test-case-repeater"

    driver = MesosSchedulerDriver(TestCaseScheduler(args.data_dir, args.output, executor, args.batch), framework, args.controller, 1)
    status = None
    if driver.run() == mesos_pb2.DRIVER_STOPPED:
        status = 0
    else:
        status = 1
    
    return status

if __name__ == '__main__':
    parser = argparse.ArgumentParser('framework')
    parser.add_argument('--offline', action='store_true')
    parser.add_argument('--batch', type=int, default=10, help="Batch size")
    parser.add_argument('data_dir', type=str, help="Data directory")
    parser.add_argument('controller', type=str, help="Controller URI")
    parser.add_argument('output', type=str, help="Output file")
    args = parser.parse_args()
    sys.exit(main(args))
