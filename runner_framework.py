#!/usr/bin/env python2.7
import argparse 
import sqlite3
import json 
import copy
import sys
import os

from threading import Thread, Lock
import mesos.interface
from mesos.interface import mesos_pb2
from mesos.scheduler import MesosSchedulerDriver

class Db(object):
    def __init__(self, dbname):
        self.dbname = dbname

    def acquire(self):
        pass

    def release(self):
        pass

    def get_db(self):
        return self.dbname

def get_tasks_size(database):
    database.acquire()
    db = sqlite3.connect(database.get_db())
    c = db.cursor()
    c.execute("select count(*) from results where vg_status == \"NOTRUN\";")
    r = c.fetchone()[0]
    database.release()
    return r

def get_tasks(database):
    """
    proglist format : id,program,args
    inputlist format: filename,stdin
    """
    results = [] 
    database.acquire()
    db = sqlite3.connect(database.get_db())
    c = db.cursor()
    c.execute("select * from results where vg_status == \"NOTRUN\" LIMIT 5;")
    res = c.fetchall()
    for r in res:
        c.execute("update results set vg_status = \"RUNNING\" where id == {i};".format(i=r[0]))
    db.commit()
    database.release()
    for r in res:
        taskid = r[0]
        program = r[2]
        program_args = r[8]
        input_filename = r[3]
        do_read = False
        if r[7] == 1:
            do_read = True
        is_stdin = False
        if r[6] == 1:
            is_stdin = True
        program_id = r[1]
        results.append((taskid,program,program_args,input_filename,do_read,is_stdin,program_id))
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
    def __init__(self, database, executor, batch):
        self.tasks = []
        self.lock = Lock()
        self.db = Db(database)
        self.cur_program_idx = 0
        self.executor = executor
        self.tasksLaunched = 0
        self.taskData = {}
        self.batchSize = batch
        self.results = 0
        self.outstanding = 0
        # Forget anything that was in flight the last time and re-run it. 
        db = sqlite3.connect(self.db.get_db())
        c = db.cursor()
        c.execute("update results set vg_status = \"NOTRUN\" where vg_status == \"RUNNING\";")
        db.commit()
        self.taskMax = get_tasks_size(self.db)

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

        print "Progress: %d(%d)/%d" % (self.results,self.outstanding,self.taskMax)
        if self.results + self.outstanding >= self.taskMax:
            print "No need for more!"
            self.lock.release()
            return

        if len(self.tasks) == 0:
            # Try to get a new set of tasks to do. 
            self.tasks = get_tasks(self.db) 
         
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

            #print "Received offer %s with cpus: %s and mem: %s" \
            #      % (offer.id.value, offerCpus, offerMem)
            
            while len(self.tasks) > 0 and remainingCpus >= 1 and remainingMem >= 1024:
                # Schedule a task. 
                tid = self.tasksLaunched
                self.tasksLaunched += 1

                #print "Launching task %d using offer %s" \
                #      % (tid, offer.id.value)
                task = mesos_pb2.TaskInfo()
                cpus = task.resources.add()
                mem = task.resources.add()
                
                task.task_id.value = str(tid)
                task.slave_id.value = offer.slave_id.value
                task.name = "task %d" % tid

                task_data_list = []
                units = []
                for i in range(0,self.batchSize):
                    if len(self.tasks) == 0:
                        continue

                    workunit = self.tasks.pop()
                    self.outstanding = self.outstanding + 1
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
                mem.scalar.value = 1024
                #task.resources = [
                #    dict(name='cpus', type='SCALAR', scalar={'value': 1}),
                #    dict(name='mem', type='SCALAR', scalar={'value': 512}),
                #]
	
                assigned_tasks.append(task)
                self.taskData[task.task_id.value] = copy.deepcopy(units)

                # This is how much we used. 
                remainingCpus = remainingCpus - 1
                remainingMem = remainingMem - 1024
            
            # Propose to launch the tasks. 
            operation = mesos_pb2.Offer.Operation()
            operation.type = mesos_pb2.Offer.Operation.LAUNCH
            operation.launch.task_infos.extend(assigned_tasks)
            driver.acceptOffers([offer.id], [operation])            
            #driver.launchTasks(offer.id, assigned_tasks)
        self.lock.release()

    def statusUpdate(s, d, u):
        def worker(self,driver,update):
            # First case, maybe the task is finished? 
            if self._finished(update.state):
                results = json.loads(update.data)
                self.lock.acquire()
                db = sqlite3.connect(self.db.get_db())
                cur = db.cursor()
                for result in results:
                    taskid = result['taskid']
                    output = result['stack']
                    status = "NOCRASH"
                    if len(output) > 0:
                        status = "CRASH"
                    cur.execute("update results set vg_status = \"{s}\" where id == {i};".format(s=status,i=taskid))
                    self.results = self.results + 1
                    self.outstanding = self.outstanding - 1
                db.commit() 
                self.lock.release()
            elif self._failed(update.state):
                print "Task failed, re-queuing"
                # If the task was killed or was lost or failed, we should re-queue it.
                units = self.taskData[update.task_id.value]

                self.lock.acquire()
                for u in units:
                    self.outstanding = self.outstanding - 1
                self.tasks.extend(units)
                self.lock.release()

            # Maybe, we have all the results?
            self.lock.acquire()
            if self.results == self.taskMax:
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

    driver = MesosSchedulerDriver(TestCaseScheduler(args.database, executor, args.batch), framework, args.controller, 1)
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
    parser.add_argument('database', type=str, help="Database")
    parser.add_argument('controller', type=str, help="Controller URI")
    args = parser.parse_args()
    sys.exit(main(args))
