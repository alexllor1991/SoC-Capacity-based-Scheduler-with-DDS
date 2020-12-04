import json
import logging
import settings
import datetime as dt

logging.basicConfig(filename=settings.LOG_FILE, level=logging.INFO, datefmt='%Y-%m-%d %H:%M:%S',
                    format='%(asctime)s:%(levelname)s:%(message)s')

class Priority(Enum):
    LOW = 0
    MEDIUM = 1
    HIGH = 2

class ServicePriorityList(object):
    def __init__(self):
        self.items = []

class ServiceList(object):
    def __init__(self):
        self.items = []

    def isServiceList(self, ser_id):
        for i in self.items:
            if i.id_ == ser_id:
                return True
            else:
                return False

    def getService(self, filter):
        for i in self.items:
            if filter(i):
                return i
            else: 
                print('Service not found')

    def getIndexService(self, filter):
        for i,x in enumerate(self.items):
            if filter(x):
                return i
            else:
                print('Service not found')

class VNFList(object):
    def __init__(self):
        self.items = []

class Service(object):

    def __init__(self, id_, n, d, p, t):

        self.id_ = id_  # service id (int)
        self.name = n   # name service (string)
        self.deadline = d   # deadline for processing a given service (datetime)
        self.priority = p  # service priority 
        self.runningtime = t # time during the service have to run (float)

        self.vnfunctions = VNFList()  # list of VNFs in a service (list)
        self.arrival_time = None  # arrival time of the service (datetime)
        self.waiting_time = None  # waiting time since the service arrived and the starting time of its first function (float)

    def isVNFList_ID(self, vnf_id):
        for i in self.vnfunctions.items:
            if i.id_ == vnf_id:
                return True
            else:
                return False

class VNFunction(object):

    def __init__(self, id_, n, r, s_id):

        self.id_ = id_  # vnf id (int: uid field in metadata deployment)
        self.name = n   # vnf name (string)
        #self.buffer_f = b    # used buffer from node (int)
        self.r_rate = r     # service rate demanded by the vnf to execute in a node
        self.serviceid = s_id  # service id (int)

        self.starting_time = None  # starting time of the function processing (datetime)
        self.completion_time = None # completion time of the function processing (datetime)
        self.processing_time = None # processing time of the function in mapped node (float)
        self.which_node = None  # node where the function is mapped

class Task(object):

    def __init__(self, id_, n, d, b, r, p):

        self.id_ = id_  # task id (int: uid field in metadata deployment)
        self.name = n   # task name (string)
        self.deadline = d   # deadline for processing a given task (datetime)
        #self.buffer_f = b   # used buffer from node (int)
        self.r_rate = r    # task rate demanded to execute this task in a node
        self.priority = p  # task priority 

        self.starting_time = None  # starting time of the task processing (datetime)
        self.completion_time = None # completion time of the task processing (datetime)
        self.processing_time = None # processing time of the task in mapped node (float)
        self.which_node = None  # node where the task is mapped