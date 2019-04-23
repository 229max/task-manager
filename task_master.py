# _*_ coding:utf-8 _*_

from nameko.rpc import rpc
from nameko.timer import timer
from nameko.standalone.rpc import ServiceRpcProxy
import time
import queue
import traceback
import threading

'''
Task manager

1. start task 
2. stop task when running (send signal to slave)
3. start task with env

author: Max.Bai
date: 2019-04
'''


# Slave status
SLAVE_STATUS_IDLE = "slave_idle"
SLAVE_STATUS_RUNNING = "slave_running"

# Task signal
TASK_SIGNAL_STOP = "stop_signal"

# Task execute enviroment
ENV_ONLINE = "ONLINE"
ENV_TEST = "TEST"
ENV_PRE = "PRE-RELEASE"

CONFIG = {'AMQP_URI': "amqp://guest:guest@localhost"}
TOKEN_LOCK = threading.Lock()

class TaskManager:
    name = "task_master"
    task_runing_queue = queue.Queue()
    task_ready_queue = []
    runner_list = []
    priority = 0

    @rpc
    def get_runner_count(self):
        return [{"name":r.name, "status":r.status} for r in self.runner_list]

    @rpc
    def start_task(self, task):
        result = {
            "code": 0,
            "msg": ""
        }
        try:
            self.check_task_data(task)
            job = Job(task["id"], task["name"], task["env"], self.priority)
            self.priority += 1
            if self.priority > 1000:
                self.priority = 0
            # self.task_ready_queue.put(job)
            self.task_ready_queue.append(job)
        except Exception as e:
            result["code"] = 1
            result["msg"] = "Start task failed. ERROR:{}".format(str(e))
        return result

    @rpc
    def stop_task(self, task):
        stoped = False
        result = {
            "code": 0,
            "msg": ""
        }
        try:
            global TOKEN_LOCK
            if TOKEN_LOCK.acquire():
                for j in self.task_ready_queue:
                    if j.name == task["name"]:
                        self.task_ready_queue.remove(j)
                        stoped = True
                        print("Stop task [{}] in queue success.".format(j.name))
                        break
                TOKEN_LOCK.release()
            

            # task in running
            if not stoped:
                payload = {
                    "signal": TASK_SIGNAL_STOP,
                    "task": task
                }
                with ServiceRpcProxy("runner_listener", CONFIG) as slave:
                    slave.send_signal.call_async(payload)
                print("Stop task [{}] in running status success.".format(task.name))
        except Exception as e:
            result["code"] = 1
            result["msg"] = "Stop task failed. ERROR:{}".format(str(e))
        return result

    @rpc
    def register_runner(self, name, env, status, heart_beat):
        if time.time() - heart_beat >= 10:
            print(name, status, heart_beat, "drop")
            return
        # print(name, status, env, heart_beat)
        new_runner = True
        for r in self.runner_list:
            if r.name == name:
                r.refresh(status, heart_beat)
                new_runner = False
                break
        if new_runner:
            self.runner_list.append(Runner(name, env, status))
            print("new runner register:", name, env)


    @timer(interval=3)
    def check_runner(self):
        '''
        1. check the runner and remove the died runner
        2. start new task when there is a runner idle
        '''
        # refresh runner list
        for i in range(len(self.runner_list)-1, -1, -1):
            if time.time() - self.runner_list[i].heart_beat >= 10:
                print("pop runner:", self.runner_list[i].name)
                self.runner_list.pop(i)
        
        # start_time = time.time()
        # run task
        # for i in range(self.task_ready_queue.qsize()):
        #     ready_task = self.task_ready_queue.get()
        #     runners = [r for r in self.runner_list if r.status == SLAVE_STATUS_IDLE and r.env == ready_task.env]
        #     if len(runners) > 0:
        #         # start run
        #         result = self.send_task_to_salve(ready_task)
        #         time.sleep(0.5) # wait slave update status
        #         if result["status"]:
        #             self.task_runing_queue.put(ready_task)
        #         else:
        #             self.task_ready_queue.put(ready_task)
        #     else:
        #         self.task_ready_queue.put(ready_task)
        # print("check cost:", time.time() - start_time)

        for r in self.runner_list:
            if r.status == SLAVE_STATUS_IDLE:
                for j in self.task_ready_queue:
                    if j.env == r.env:
                        # start run
                        result = self.send_task_to_salve(j)
                        time.sleep(0.5) # wait slave update status
                        if result["status"]:
                            self.task_ready_queue.remove(j)
                            self.task_runing_queue.put(j)
                            break




    # @timer(interval=3)
    # def check_running_task(self):
    #     # clear running task
    #     if not self.task_runing_queue.empty():
    #         for i in range(self.task_runing_queue.qsize()):
    #             running_task = self.task_runing_queue.get()
    #             if running_task["name"] != task["name"]:
    #                 self.task_runing_queue.put(ready_task)
    #             else:
    #                 stoped = True

    def send_task_to_salve(self, job):
        result = {
            "status": True,
            "msg": "Send task to slave success."
        }
        try:
            print("Send task [{}] to [{}] slave".format(job.name, job.env))
            with ServiceRpcProxy("runner_slave_{}".format(job.env), CONFIG) as slave:
                slave.run_task.call_async(job.to_dict())
        except Exception as e:
            result["status"] = False
            result["msg"] = "Send task to slave failed. ERROR:{}".format(str(e))
            traceback.print_exc()
        return result
                
    def check_task_data(self, task):
        if not task.get('env', None) in [ENV_ONLINE, ENV_PRE, ENV_TEST]:
            raise('Task data <env> value invalid!')


class Runner:
    def __init__(self, name, env, status):
        self.name = name
        self._status = status
        self._env = env
        self.heart_beat = time.time()
    
    @property
    def status(self):
        return self._status

    @status.setter
    def status(self, value):
        self._status = value

    @property
    def env(self):
        return self._env

    def refresh(self, status, heart_beat):
        self.heart_beat = heart_beat
        self._status = status

class Job:
    def __init__(self, id, name, env, priority):
        self._id = id
        self._name = name
        self._env = env
        self._priority = priority
    
    def __lt__(self, other):
        return self.priority < other.priority
    
    @property
    def priority(self):
        return self._priority
    
    # @priority.setter
    # def priority(self, value):
    #     self._priority = value

    @property
    def name(self):
        return self._name
    
    @property
    def id(self):
        return self._id
    
    @property
    def env(self):
        return self._env

    def to_dict(self):
        return {
            "id": self.id,
            "name": self.name,
            "env": self.env
        }
