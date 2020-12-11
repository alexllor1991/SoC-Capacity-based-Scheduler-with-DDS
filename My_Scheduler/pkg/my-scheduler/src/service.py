import json
import logging
import settings
from enum import Enum
from kubernetes import client, config

logging.basicConfig(filename=settings.LOG_FILE, level=logging.INFO, datefmt='%Y-%m-%d %H:%M:%S',
                    format='%(asctime)s:%(levelname)s:%(message)s')

class ServiceList(object):
    def __init__(self):
        self.items = []

    def isServiceList(self, ser_id):
        if len(self.items) > 0:
            for i in self.items:
                if i.id_ == ser_id:
                    return True
                else:
                    return False
        else:
            return False

    def getService(self, filter):
        for i in self.items:
            if filter(i):
                return i
            #else: 
            #    print('Service not found')

    def getIndexService(self, filter):
        for i,x in enumerate(self.items):
            if filter(x):
                return i
            #else:
            #    print('Service not found')

class VNFList(object):
    def __init__(self):
        self.items = []

    def isVNFList_ID(self, vnf_id):
        if len(self.items) > 0:
            for i in self.items:
                if i.id_ == vnf_id:
                    return True
                else:
                    return False
        else:
            return False

    def getVNF(self, filter):
        for i in self.items:
            if filter(i):
                return i
            #else: 
            #    print('VNF not found')

    def getIndexVNF(self, filter):
        for i,x in enumerate(self.items):
            if filter(x):
                return i

class Service(object):

    def __init__(self, id_, n, d, p, tr_):

        self.id_ = id_  # service id (int)
        self.name = n   # name service (string)
        self.deadline = d   # deadline for processing a given service (datetime)
        self.priority = p  # service priority 
        self.running_time = tr_ # time during the service have to run (float)
        self.vnfunctions = VNFList()  # list of VNFs in a service (list)
        self.arrival_time = None  # arrival time of the service (datetime)
        self.waiting_time_first_VNF = None  # waiting time since the service arrived and the starting time of its first function (float)

    def to_dir(self):
        print( \
            {
                'Id': self.id_, 
                'Name': self.name, 
                'Deadline': str(self.deadline), 
                'Priority': self.priority,
                'Running_time': self.running_time,
                'VNFunctions': self.vnfunctions.items,
                'Arrival_time': str(self.arrival_time),
                'Waiting_time_First_VNF': self.waiting_time_first_VNF,
            })

class VNFunction(object):

    def __init__(self, id_, n, r, s_id):

        self.id_ = id_  # vnf id (int: uid field in metadata deployment)
        self.name = n   # vnf name (string)
        self.r_rate = r     # service rate demanded by the vnf to execute in a node
        self.serviceid = s_id  # service id (int)

        self.running_time = None  # amount time to run in node
        self.deadline = None
        self.service_arrival_time = None
        self.priority = None
        self.starting_time = None  # starting time of the VNF processing in node (datetime)
        self.completion_time = None # completion time of the VNF processing in node (datetime)
        #self.processing_time = None # processing time of the function in mapped node (float)
        self.waiting_time = None  # waiting time of the VNF to be processed (arrival_time_first_VNF - current_time)  (float)
        self.in_node = None  # node where the function is mapped

    def to_dir(self):
        print( \
            {
                'Id': self.id_, 
                'Name': self.name,
                'Demanded_rate': self.r_rate,
                'Service_id': self.serviceid,
                'Running_time': self.running_time,
                'Deadline': str(self.deadline),
                'Service_arrival_time': str(self.service_arrival_time),
                'Priority': self.priority,
                'Starting_time': str(self.starting_time),
                'Completion_time': str(self.completion_time),
                'Waiting_time': self.waiting_time,
                'Assigned_node': self.in_node,
            })

class Task(object):

    def __init__(self, id_, n, d, tr_, b, r, p):

        self.id_ = id_  # task id (int: uid field in metadata deployment)
        self.name = n   # task name (string)
        self.deadline = d   # deadline for processing a given task (datetime)
        self.running_time_task = tr_
        self.r_rate = r    # task rate demanded to execute this task in a node
        self.priority = p  # task priority 

        self.starting_time = None  # starting time of the task processing (datetime)
        self.completion_time = None # completion time of the task processing (datetime)
        self.processing_time = None # processing time of the task in mapped node (float)
        self.which_node = None  # node where the task is mapped

    def to_dir(self):
        print( \
            {
                'Id': self.id_, 
                'Name': self.name,
                'Demanded_rate': self.r_rate,
                'Running_time': self.running_time_task,
                'Deadline': str(self.deadline),
                'Service_arrival_time': str(self.service_arrival_time),
                'Priority': self.priority,
                'Starting_time': str(self.starting_time),
                'Completion_time': str(self.completion_time),
                'Processing_time': self.processing_time,
                'Assigned_node': self.which_node,
            })