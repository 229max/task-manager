from nameko.standalone.rpc import ClusterRpcProxy
import random

CONFIG = {'AMQP_URI': "amqp://guest:guest@localhost"}


def multi_task():
    with ClusterRpcProxy(CONFIG) as rpc:
        id = 0
        while id < 100:
            id += 1
            rpc.task_master.start_task({"id":id,"name":"task_{}".format(id),"env":random.choice(["TEST","PRE-RELEASE"])})
        

if __name__ == "__main__":
    multi_task()
