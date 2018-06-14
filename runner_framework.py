#!/usr/bin/env python2.7
import argparse 
import json 
import csv
import sys
import os

from runner_core.runner import stack_from_xml
import mesos.interface
from mesos.interface import mesos_pb2
from mesos.scheduler import MesosSchedulerDriver

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


class TestCaseScheduler(mesos.interface.Scheduler):
    def __init__(self, tasks, output, executor):
        self.tasks = tasks
        self.taskNum = len(tasks)
        self.executor = executor
        self.tasksLaunched = 0
        self.taskData = {}
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
        return r

    def registered(self, driver, frameworkId, masterInfo):
        print "Registered with framework ID %s" % frameworkId.value

    def resourceOffers(self, driver, offers):
        for offer in offers:
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
                workunit = self.tasks.pop()

                print "Launching task %d using offer %s" \
                      % (tid, offer.id.value)

                task = mesos_pb2.TaskInfo()
                cpus = task.resources.add()
                mem = task.resources.add()
                
                task.task_id.value = str(tid)
                task.slave_id.value = offer.slave_id.value
                task.name = "task %d" % tid
                task_data = {}
                task_data['taskid'] = workunit[0]
                task_data['program'] = "file://{}".format(workunit[1])
                task_data['program_args'] = workunit[2]
                task_data['input_filename'] = "file://{}".format(workunit[3])
                task_data['do_read'] = workunit[4]
                task_data['stdin'] = workunit[5]
                task_data['program_id'] = workunit[6]
                task.data = json.dumps(task_data)
                task.executor.MergeFrom(self.executor)
                """
                commandinfo = mesos_pb2.CommandInfo()

                proginfo = commandinfo.uris.add()
                proginfo.value = "file://{}".format(workunit[1])

                inputinfo = commandinfo.uris.add()
                inputinfo.value = "file://{}".format(workunit[3])

                task.command.MergeFrom(commandinfo)
                """

                cpus.name = "cpus"
                cpus.type = mesos_pb2.Value.SCALAR
                cpus.scalar.value = 1
                mem.name = "mem"
                mem.type = mesos_pb2.Value.SCALAR
                mem.scalar.value = 512

                assigned_tasks.append(task)
                self.taskData[task.task_id.value] = (offer.slave_id, task.executor.executor_id)

                # This is how much we used. 
                remainingCpus = remainingCpus - 1
                remainingMem = remainingMem - 512
            
            # Propose to launch the tasks. 
            operation = mesos_pb2.Offer.Operation()
            operation.type = mesos_pb2.Offer.Operation.LAUNCH
            operation.launch.task_infos.extend(assigned_tasks)
            driver.acceptOffers([offer.id], [operation])

    def statusUpdate(self, driver, update):
        # First case, maybe the task is finished? 
        if self._finished(update.state):
            results = json.loads(update.data)
            stack = stack_from_xml(results['stack'])
            if stack == None:
                stack = "-nocrash-"
            self.outwriter.writerow([results['hash'], results['inputfile'], "-".join(stack)])
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

    """
    container = mesos_pb2.ContainerInfo()
    docker_container = mesos_pb2.ContainerInfo.DockerInfo()
    docker_container.image = "grinder"
    container.type = mesos_pb2.ContainerInfo.DOCKER
    container.docker.MergeFrom(docker_container)
    """

    # Get the path to our executor
    executor_path = os.path.abspath("./executor")
    # Then, boot up all the Mesos crap and get us registered with the framework. 

    executor = mesos_pb2.ExecutorInfo()
    executor.executor_id.value = "test-case-executor"
    executor.command.value = executor_path
    executor.name = "Test case repeater"
    #executor.container.MergeFrom(container)

    framework = mesos_pb2.FrameworkInfo()
    framework.user = "" # Have Mesos fill in the current user.
    framework.name = "Test case repeater framework"
    framework.checkpoint = True
    framework.principal = "test-case-repeater"

    driver = MesosSchedulerDriver(TestCaseScheduler(task_list, args.output, executor), framework, args.controller, 1)
    status = None
    if driver.run() == mesos_pb2.DRIVER_STOPPED:
        status = 0
    else:
        status = 1
    
    return status

if __name__ == '__main__':
    parser = argparse.ArgumentParser('framework')
    parser.add_argument('data_dir', type=str, help="Data directory")
    parser.add_argument('controller', type=str, help="Controller URI")
    parser.add_argument('output', type=str, help="Output file")
    args = parser.parse_args()
    sys.exit(main(args))
