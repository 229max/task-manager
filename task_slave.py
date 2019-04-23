from nameko.rpc import rpc
from nameko.standalone.rpc import ServiceRpcProxy
from nameko.timer import timer
from nameko.events import EventDispatcher, event_handler, BROADCAST
import time
import threading
import uuid
import random

from test_nameko_master import SLAVE_STATUS_IDLE, SLAVE_STATUS_RUNNING, TASK_SIGNAL_STOP, ENV_ONLINE, ENV_PRE, ENV_TEST

'''
Task slave

author: Max.Bai
date: 2019-04

1. start slave with diffrent enviroment
2. handle task signal
3. heartbeat every 5s
'''


CONFIG = {'AMQP_URI': "amqp://guest:guest@localhost"}
SLAVE_ENV = ENV_TEST
SLAVE_NAME = "runner_slave_{}_{}".format(SLAVE_ENV, uuid.uuid1())

class TestTask():
    name = ""
    signal = ""
    status = SLAVE_STATUS_IDLE

slave_task = TestTask()

class TaskRunnerSlave(object):
    name = "runner_slave_{}".format(SLAVE_ENV)
    status = SLAVE_STATUS_IDLE

    @rpc
    def run_task(self, task:dict):
        print("run task...")
        self.status = SLAVE_STATUS_RUNNING
        slave_task.name = task["name"]
        slave_task.signal = ""
        slave_task.status = self.status
        self.task_duration_timeout = False
        self.cancel_task = False
        self.heart_beat()

        # start runing task
        self.start_locust(task)

        # complete task initial status
        self.status = SLAVE_STATUS_IDLE
        self.task_duration_timeout = False
        self.cancel_task = False
        slave_task.status = self.status
        # return "starting ...."

    def start_locust(self, task):
        print("start locust...", task["name"])
        duration = random.randint(10, 50)
        start_time = time.time()
        print("task duration:", duration)
        while not self.check_running(start_time, duration):
            time.sleep(2)
        if self.cancel_task:
            # save cancle task
            print("cancel task:", task["name"])
            pass
        else:
            # save complete task 
            print("task done:", task["name"])
            pass
        print("task runing complete.", task["name"], "cost:", time.time()-start_time)
    

    def check_running(self, start_time, duration):
        if (time.time() - start_time) > duration:
            self.task_duration_timeout = True
        if slave_task.signal == TASK_SIGNAL_STOP:
            self.cancel_task = True
        return self.task_duration_timeout or self.cancel_task
    
    def heart_beat(self):
        with ServiceRpcProxy("task_master", CONFIG) as master:
            master.register_runner.call_async(SLAVE_NAME, SLAVE_ENV, self.status, time.time())
        

class TaskRunnerListener(object):
    """Slave listener to handler the signal like stop cancel...
    """
    name = "runner_listener"
    dispatch = EventDispatcher()
    print("-"*30)
    print("| SLAVE ID:{}".format(SLAVE_NAME))
    print("| SLAVE ENV:{}".format(SLAVE_ENV))
    print("-"*30)
    @rpc
    def send_signal(self, payload):
        if payload["signal"] == TASK_SIGNAL_STOP:
            self.dispatch("stop_task", payload)

    @event_handler("runner_listener", "stop_task", handler_type=BROADCAST, reliable_delivery=False)
    def handle_stop_task(self, payload):
        if slave_task.name == payload["task"]["name"]:
            slave_task.signal = TASK_SIGNAL_STOP
            print("stopping task ...:", payload)
    
    @timer(interval=5)
    def heart_beat(self):
        with ServiceRpcProxy("task_master", CONFIG) as master:
            master.register_runner.call_async(SLAVE_NAME, SLAVE_ENV, slave_task.status, time.time())


