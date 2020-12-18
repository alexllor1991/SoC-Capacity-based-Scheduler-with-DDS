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

    def isServiceList(self, filter):
        found = False
        if len(self.items) > 0:
            for i in self.items:
                if filter(i):
                    found = True
            if found: 
                return True
            else: 
                return False
        else:
            return False

    def getService(self, filter):
        for i in self.items:
            if filter(i):
                return i

    def getIndexService(self, filter):
        for i,x in enumerate(self.items):
            if filter(x):
                return i

class VNFList(object):
    def __init__(self):
        self.items = []

    def isVNFList(self, filter):
        found = False
        if len(self.items) > 0:
            for i in self.items:
                if filter(i):
                    found = True
            if found:
                return True
            else:
                return False
        else:
            return False

    def getVNF(self, filter):
        for i in self.items:
            if filter(i):
                return i

    def getIndexVNF(self, filter):
        for i,x in enumerate(self.items):
            if filter(x):
                return i

    def areAllVNFScheduled(self, filter):
        count = 0
        for i in self.items:
            if filter(i):
                count += 1
                #print('Scheduled VNF ' + str(count) + 'of ' + str(len(self.items)))
        if count == len(self.items):
            #print('All VNFs have been scheduled')
            return True
        else:
            return False

class TaskList(object):
    def __init__(self):
        self.items = []

    def isTaskList(self, filter):
        found = False
        if len(self.items) > 0:
            for i in self.items:
                if filter(i):
                    found = True
            if found:
                return True
            else:
                return False
        else:
            return False

    def getTask(self, filter):
        for i in self.items:
            if filter(i):
                return i

    def getIndexTask(self, filter):
        for i,x in enumerate(self.items):
            if filter(x):
                return i

class Service(object):

    def __init__(self, id_, n, d, p, tr_):
        """
        :param Service Id:
        :param Service name:
        :param Service deadline:
        :param Service priority:
        :param Service running time:
        :return:
        """
        self.id_ = id_  # service id (int)
        self.name = n   # name service (string)
        self.deadline = d   # deadline for processing a given service (datetime)
        self.priority = p  # service priority 
        self.running_time = tr_ # time during the service have to run (float)
        self.vnfunctions = VNFList()  # list of VNFs in a service (list)
        
        self.arrival_time = None  # arrival time of the service (datetime)
        self.waiting_time_first_VNF = None  # waiting time since the service arrived and the starting time of its first function (float)
        self.makespan = None # total time of the service in the system (completion_time_lastVNF - arrival_time) (float)

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
                'Makespan_Service': self.makespan,
            })

class VNFunction(object):

    def __init__(self, id_, n, r, s_id):
        """
        :param VNF Id:
        :param VNF name:
        :param VNF demanded rate:
        :param Service Id:
        :return:
        """
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
        self.makespan = None  # total time of the VNF in the system (completion_time - starting_time) (float)

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
                'Makespan_VNF': self.makespan,
            })

class Task(object):

    def __init__(self, id_, n, d, p, tr_, r):
        """
        :param Task Id:
        :param Task name:
        :param Task deadline:
        :param Task priority:
        :param Task running time:
        :param Task demanded rate:
        :return:
        """
        self.id_ = id_  # task id (int: uid field in metadata deployment)
        self.name = n   # task name (string)
        self.deadline = d   # deadline for processing a given task (datetime)
        self.priority = p  # task priority
        self.running_time = tr_   # running time of the task
        self.r_rate = r    # task rate demanded to execute this task in a node

        self.task_arrival_time = None  # arrival time of the task (datetime)
        self.starting_time = None  # starting time of the task processing in node (datetime)
        self.completion_time = None # completion time of the task processing (datetime)
        #self.processing_time = None # processing time of the task in mapped node (float)
        self.waiting_time = None  # waiting time of the task to be processed (arrival_time - current_time)  (float)
        self.in_node = None  # node where the task is mapped
        self.execution_time = None # execution time of the task (completion_time - starting_time) (float)
        self.flow_time = None  # total time of the task in the system (completion_time - task_arrival_time) (float)

    def to_dir(self):
        print( \
            {
                'Id': self.id_, 
                'Name': self.name,
                'Demanded_rate': self.r_rate,
                'Running_time': self.running_time,
                'Deadline': str(self.deadline),
                'Task_arrival_time': str(self.task_arrival_time),
                'Priority': self.priority,
                'Starting_time': str(self.starting_time),
                'Completion_time': str(self.completion_time),
                'Waiting_time': self.waiting_time,
                'Assigned_node': self.in_node,
                'Execution_time': self.execution_time,
                'Flow_time': self.flow_time,
            })