#!/usr/bin/env python2.7

import sys

import mesos.interface
from mesos.interface import mesos_pb2
from mesos.executor import MesosExecutorDriver

class TestCaseExecutor(mesos.interface.Executor):
    """
    If this becomes a 1:N mapping for a queue of test cases, then 
    we should add some logic that will surrender tasks when they are 
    done. I don't know if we need that, though. 
    """
    def launchTask(self, driver, task):
        """
        Figure out exactly what task we got, and start it. 
        """
        pass

if __name__ == "__main__":
    driver = MesosExecutorDriver(TestCaseExecutor())
    sys.exit(0 if driver.run() == mesos_pb2.DRIVER_STOPPED else 1)
