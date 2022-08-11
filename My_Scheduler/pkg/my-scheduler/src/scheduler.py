import os
import csv
import time
#from time import sleep
from datetime import datetime, timedelta
from dateutil import parser
from threading import Thread
import multiprocessing 
import logging
import pandas as pd
import statsmodels.formula.api as smf
from sklearn import metrics
import numpy as np
import settings
import itertools
from monitor import ClusterMonitor
from node import NodeList
from pod import DataType, Pod, PodList
from service import ServiceList, TaskList, Service, VNFunction, Task
from dds import DDS_Algo

#import rti.connextdds as dds_package

from kubernetes import client, config, watch
from kubernetes.client.rest import ApiException

logging.basicConfig(filename=settings.LOG_FILE, level=logging.INFO, datefmt='%Y-%m-%d %H:%M:%S',
                    format='%(asctime)s:%(levelname)s:%(message)s')

pd.options.mode.chained_assignment = None

class Scheduler:
    def __init__(self):
        self.monitor = ClusterMonitor()
        self.localdds = "kubernetes-control-plane1"
        self.dds = DDS_Algo("k8s_master", self.localdds, "GC_UPC_1")
        self.watcherpod = watch.Watch()
        configuration = client.Configuration()
        self.v1 = client.CoreV1Api(client.ApiClient(configuration))
        self.batch_v1 = client.BatchV1Api(client.ApiClient(configuration))
        self.apps_v1 = client.AppsV1Api(client.ApiClient(configuration))
        self.priority_list = multiprocessing.Manager().list()
        self.time_interval_multideployment = settings.TIME_INTERVAL_MULTIDEPLOYMENTS

        self.vnf = VNFunction(None,None,None,None,None)
        self.task = Task(None, None, None, None, None, None)

        self.scheduler_name = 'my-scheduler'

        self.fieldnames_results = ['Timestamp',
                                   'Requested_services',
                                   'Scheduled_services',
                                   'Rejected_services',
                                   'Requested_VNFs',
                                   'Scheduled_VNFs',
                                   'Rejected_VNFs',
                                   'Requested_tasks',
                                   'Scheduled_tasks',
                                   'Rejected_tasks',
                                   'Failed_VNFs',
                                   'Failed_tasks',
                                   'Deadline_violations']

        self.fieldnames_prediction_results = ['Timestamp',
                                              'R_Squared',
                                              'MAE',
                                              'MSE',
                                              'RMSE',
                                              'Model']

        self.pred_model = None

    def run(self):
        """
        Main thread, run and listen for events,
        If an event occurred, call monitor.update_nodes()
        and proceed scoring and scheduling process.
        """
        print('Scheduler running')

        p1 = Thread(target=self.monitor.monitor_runner)
        p1.start()

        p2 = Thread(target=self.scheduler_runner)
        p2.start()

        self.init_files()

        p3 = Thread(target=self.monitor.monitor_SoC_nodes)
        p3.start()

        if (settings.SCHEDULING_CRITERIA == 0 or settings.SCHEDULING_CRITERIA == 1):
            p4 = Thread(target=self.check_model)
            p4.start()

        p5 = Thread(target=self.update_GC)
        p5.start()

        p6 = Thread(target=self.dds.read_samples, args=(self.monitor, self,))
        p6.start()
        
        while True:
            try:
                for eventpod in self.watcherpod.stream(self.v1.list_pod_for_all_namespaces):    

                    if eventpod['type'] == 'ADDED' and eventpod['object'].spec.scheduler_name == self.scheduler_name:
                        event_name = str(eventpod['object'].metadata.name)
                        if eventpod['object'].metadata.annotations['event'] == "service" or event_name.startswith('vnf'):
                            if not self.monitor.all_services.isServiceList(lambda x: x.id_ == eventpod['object'].metadata.annotations['service_id']):
                                new_service = Service(eventpod['object'].metadata.annotations['service_id'], eventpod['object'].metadata.annotations['service_name'], eventpod['object'].metadata.annotations['service_deadline'], eventpod['object'].metadata.annotations['service_priority'], eventpod['object'].metadata.annotations['service_running_time'], eventpod['object'].metadata.annotations['amount_vnfs'], eventpod['object'].metadata.annotations['multideployment'])
                                print ('New service ADDED', new_service.name)
                                new_service.arrival_time = datetime.now()
                                self.monitor.all_services.items.append(new_service)
                                self.monitor.requested_services += 1
                            serv = self.monitor.all_services.getService(lambda x: x.id_ == eventpod['object'].metadata.annotations['service_id'])
                        
                            if not serv.vnfunctions.isVNFList(lambda x: x.id_ == eventpod['object'].metadata.annotations['vnf_id']):
                                new_vnf = VNFunction(eventpod['object'].metadata.annotations['vnf_id'], eventpod['object'].metadata.annotations['vnf_name'], eventpod['object'].metadata.annotations['required_service_rate'], eventpod['object'].metadata.annotations['service_id'], eventpod['object'].metadata.annotations['service_name'])
                                new_vnf.deadline = parser.parse(serv.deadline)
                                new_vnf.priority = serv.priority
                                new_vnf.running_time = serv.running_time
                                new_vnf.service_arrival_time = serv.arrival_time
                                new_vnf.multideployment = serv.multideployment
                                self.monitor.update_events_file(eventpod['object'].metadata.annotations['event'], new_vnf.name, new_vnf.id_, eventpod['object'].metadata.annotations['service_id'], eventpod['object'].metadata.annotations['service_name'], new_vnf.service_arrival_time, None, new_vnf.r_rate, new_vnf.running_time, new_vnf.deadline, new_vnf.priority, None, None, None, None, None, None)
                                serv.vnfunctions.items.append(new_vnf)
                                serv.vnfunctions.items.sort(key=lambda x: x.id_, reverse=False)
                                self.monitor.requested_VNFs += 1

                            self.vnf = serv.vnfunctions.getVNF(lambda x: x.id_ == eventpod['object'].metadata.annotations['vnf_id'])

                            index = self.monitor.all_services.getIndexService(lambda x: x.id_ == serv.id_)
                            self.monitor.all_services.items[index] = serv

                        elif eventpod['object'].metadata.annotations['event'] == "task" or event_name.startswith('task'):
                            if not self.monitor.all_tasks.isTaskList(lambda x: x.id_ == eventpod['object'].metadata.annotations['task_id']):
                                new_task_deadline = parser.parse(eventpod['object'].metadata.annotations['task_deadline'])
                                new_task = Task(eventpod['object'].metadata.annotations['task_id'], eventpod['object'].metadata.annotations['task_name'], new_task_deadline, eventpod['object'].metadata.annotations['task_priority'], eventpod['object'].metadata.annotations['task_running_time'], eventpod['object'].metadata.annotations['task_required_rate'])
                                print ('New task ADDED', new_task.name)
                                new_task.task_arrival_time = datetime.now()
                                self.monitor.update_events_file(eventpod['object'].metadata.annotations['event'], new_task.name, new_task.id_, None, None, None, new_task.task_arrival_time, new_task.r_rate, new_task.running_time, new_task.deadline, new_task.priority, None, None, None, None, None, None)
                                self.monitor.all_tasks.items.append(new_task)
                                self.monitor.requested_tasks += 1
                            self.task = self.monitor.all_tasks.getTask(lambda x: x.id_ == eventpod['object'].metadata.annotations['task_id'])

                        else:
                            print('None event was detected')
                            continue

                        new_pod = Pod(eventpod['object'].metadata, eventpod['object'].spec, eventpod['object'].status)
                        new_pod.scheduling_criteria = settings.SCHEDULING_CRITERIA

                        if eventpod['object'].metadata.annotations['event'] == 'service' or event_name.startswith('vnf'):
                            new_pod.demanded_processing = self.vnf.r_rate
                            new_pod.id = self.vnf.id_
                            new_pod.service_id = self.vnf.serviceid
                            new_pod.deadline = self.vnf.deadline
                            new_pod.running_time = self.vnf.running_time
                            new_pod.service_arrival_time = self.vnf.service_arrival_time
                            new_pod.priority = self.vnf.priority
                            new_pod.event = 'service'

                            if self.vnf.multideployment:
                                self.monitor.multideployed_pods.items.append(new_pod)
                                deadline = abs((new_pod.deadline - datetime.now() - timedelta(seconds=int(new_pod.running_time))).seconds)
                                data = {}
                                data["Identificador"] = self.vnf.servicename
                                data["NodeId"] = new_pod.metadata.name
                                data["TerminationPointId"] = str(new_pod.parse_usage_data(new_pod.spec.containers[0].resources.requests['cpu'], DataType.CPU))
                                data["LinkId"] = str(self.vnf.running_time)
                                data["SourceNodeTp"] = str(deadline)
                                data["DestinationNodeTp"] = str(eventpod['object'].metadata.annotations['amount_vnfs'])
                                data["SourceNode"] = str(self.vnf.multideployment)
                                data["DestinationNode"] = self.localdds
                                
                                self.dds.lock.acquire()
                                writer = self.dds.connector.get_output("kubernetes-control-plane1-pub::kubernetes-control-plane1-dw")
                                writer.instance.set_dictionary(data)
                                dt = int(datetime.now().timestamp() * 1000000000)
                                writer.write(source_timestamp=dt)
                                self.dds.lock.release()
                        
                        elif eventpod['object'].metadata.annotations['event'] =='task' or event_name.startswith('task'):
                            new_pod.demanded_processing = self.task.r_rate
                            new_pod.id = self.task.id_
                            new_pod.deadline = self.task.deadline
                            new_pod.running_time = self.task.running_time
                            new_pod.task_arrival_time = self.task.task_arrival_time
                            new_pod.priority = self.task.priority
                            new_pod.event = 'task'
                        else:
                            print('None event was detected')
                        
                        self.monitor.status_lock.acquire(blocking=True)

                        if not self.monitor.all_pods.isPodList(lambda x: x.metadata.name == new_pod.metadata.name):
                            self.monitor.all_pods.items.append(new_pod)
                            print('New pod ADDED', new_pod.metadata.name)
                        else:
                            index_pod = self.monitor.all_pods.getIndexPod(lambda x: x.metadata.name == new_pod.metadata.name)
                            self.monitor.all_pods.items[index_pod] = new_pod
                            print('UPDATED pod', new_pod.metadata.name)

                        self.monitor.status_lock.release()

                        if not self.vnf.multideployment:
                            if settings.PRIORITY_LIST_CRITERIA == 0: # priority list is sorted by taking into account the service deadline
                                prov_list = list(self.priority_list)
                                prov_list.append(new_pod)
                                prov_list.sort(key=lambda x: x.deadline, reverse=False)
                                self.priority_list = prov_list.copy()
                            elif settings.PRIORITY_LIST_CRITERIA == 1: # priority list is sorted by taking into account the service priority
                                prov_list = list(self.priority_list)
                                prov_list.append(new_pod)
                                prov_list.sort(key=lambda x: x.priority, reverse=True)
                                self.priority_list = prov_list.copy()
                            elif settings.PRIORITY_LIST_CRITERIA == 2: # priority list is sorted dynamically by taking into account an equation
                                task_delay = abs((new_pod.deadline - datetime.now() - timedelta(seconds=int(new_pod.running_time))).seconds)
                                if new_pod.event == 'service': 
                                    waiting_time = abs((datetime.now() - new_pod.service_arrival_time).seconds)
                                elif new_pod.event == 'task':
                                    waiting_time = abs((datetime.now() - new_pod.task_arrival_time).seconds)
                                else:
                                    waiting_time = 0
                                rank = (settings.delta_1 * task_delay) - ((1 - settings.delta_1) * waiting_time)
                                new_pod.rank = rank
                                prov_list = list(self.priority_list)
                                prov_list.append(new_pod)
                                prov_list.sort(key=lambda x: x.rank, reverse=False)
                                self.priority_list = prov_list.copy()
                            else:
                                print('None priority list criteria has been set')

            except Exception as e:
                print(str(e))

    def init_files(self):
        """
        Initialize log file, insert node names at
        the first line of a file
        :return:
        """
        self.monitor.update_nodes()

        with open('Usage.csv', 'w', buffering=1) as csvfile1:
            writer1 = csv.DictWriter(csvfile1, fieldnames=self.monitor.fieldnames_usage_node)
            writer1.writeheader()

        with open('Results.csv', 'w', buffering=1) as csvfile2:
            writer2 = csv.DictWriter(csvfile2, fieldnames=self.fieldnames_results)
            writer2.writeheader()

        with open('Events.csv', 'w', buffering=1) as csvfile3:
            writer3 = csv.DictWriter(csvfile3, fieldnames=self.monitor.fieldnames_events)
            writer3.writeheader()

        if (settings.SCHEDULING_CRITERIA == 0 or settings.SCHEDULING_CRITERIA == 1):
            with open('Predictions_results.csv', 'w', buffering=1) as csvfile4:
                writer4 = csv.DictWriter(csvfile4, fieldnames=self.fieldnames_prediction_results)
                writer4.writeheader()

    def update_result_files(self):
        '''
        Saves statistics of the scheduling process to Results.csv file
        '''
        with open('Results.csv', 'a', buffering=1) as csvfile2:
            writer2 = csv.DictWriter(csvfile2, fieldnames=self.fieldnames_results)
            writer2.writerow({'Timestamp': str(datetime.now()),
                             'Requested_services': str(self.monitor.requested_services),
                             'Scheduled_services': str(self.monitor.scheduled_services),
                             'Rejected_services': str(self.monitor.rejected_services),
                             'Requested_VNFs': str(self.monitor.requested_VNFs),
                             'Scheduled_VNFs': str(self.monitor.scheduled_VNFs),
                             'Rejected_VNFs': str(self.monitor.rejected_VNFs),
                             'Requested_tasks': str(self.monitor.requested_tasks),
                             'Scheduled_tasks': str(self.monitor.scheduled_tasks),
                             'Rejected_tasks': str(self.monitor.rejected_tasks),
                             'Failed_VNFs': str(self.monitor.failed_vnfs),
                             'Failed_tasks': str(self.monitor.failed_tasks),
                             'Deadline_violations': str(self.monitor.deadline_violations)})

    def scheduler_runner(self):
        '''
        Runs the scheduler process while existing events in the priority queue
        '''
        while True:
            if len(self.priority_list) > 0:
                try:

                    self.monitor.update_nodes()

                    self.monitor.update_usage_file()

                    self.update_result_files()

                    obj = self.priority_list.pop(0)

                    if settings.DEPLOY_USING_NATIVE_SCHEDULER is True:
                        if self.monitor.all_pods.isPodList(lambda x: x.metadata.name == obj.metadata.name):
                            candidate_node = NodeList()
                            for node in self.monitor.all_nodes.items:
                                if node.spec.unschedulable is not True:
                                    candidate_node.items.append(node)
                            if len(candidate_node.items) == 0 or (len(candidate_node.items) == 1 and candidate_node.isNodeList(lambda x: x.metadata.name == settings.MASTER_NAME)):
                                print('None node can meet the %s pod requirements' % obj.metadata.name)
                                if obj.event == "service":
                                    if self.monitor.all_services.isServiceList(lambda x: x.id_ == obj.service_id):
                                        serv = self.monitor.all_services.getService(lambda x: x.id_ == obj.service_id)
                                        numberVNFs = len(serv.vnfunctions.items)

                                        self.monitor.status_lock.acquire(blocking=True)

                                        self.monitor.all_services.items.remove(serv)
                                        for i in self.monitor.all_pods.items:
                                            if i.service_id == serv.id_:
                                                self.monitor.all_pods.items.remove(i)
                                                print('Pod %s deleted' % i.metadata.name)

                                        provi_list = list(self.priority_list)
                                        for i in provi_list:
                                            if i.service_id == serv.id_:
                                                provi_list.remove(i)
                                        self.priority_list = provi_list.copy()

                                        self.monitor.status_lock.release()

                                        vnf_serv_scheduled = 0
                                        for i in serv.vnfunctions.items:
                                            if i.in_node is not None:
                                                vnf_serv_scheduled += 1

                                        self.monitor.rejected_services += 1
                                        self.monitor.rejected_VNFs += numberVNFs - vnf_serv_scheduled

                                        for i in serv.vnfunctions.items:
                                            if int(serv.running_time) > 0:
                                                self.monitor.delete_job(i.name)
                                            else:
                                                self.monitor.delete_deployment(i.name)

                                        self.update_result_files()

                                elif obj.event == "task":
                                    task = self.monitor.all_tasks.getTask(lambda x: x.id_ == obj.id)
                                    self.monitor.delete_job(task.name)

                                    self.monitor.status_lock.acquire(blocking=True)

                                    self.monitor.all_tasks.items.remove(task)
                                    self.monitor.all_pods.items.remove(obj)
                                    print('Pod %s deleted' % obj.metadata.name)

                                    self.monitor.status_lock.release()

                                    self.monitor.rejected_tasks += 1

                                    self.update_result_files()
                                else:
                                    print('Error! Some event must be detected')          
                            else:
                                if int(obj.running_time) > 0:
                                    self.pass_to_scheduler(obj.metadata.labels['app'], 'default', 'default-scheduler', 'job')
                                else:
                                    self.pass_to_scheduler(obj.metadata.labels['app'], 'default', 'default-scheduler', 'deployment')
                                pod = None
                                resp = None
                                pod_updated = False
                                while not pod_updated:
                                    for p in self.v1.list_pod_for_all_namespaces().items:
                                        if p.metadata.name == obj.metadata.name:
                                            pass
                                        else:
                                            pod_updated = True
                                for p in self.v1.list_pod_for_all_namespaces().items:
                                    if p.metadata.labels['app'] == obj.metadata.labels['app']:
                                        pod = self.monitor.all_pods.getPod(lambda x: x.metadata.name == obj.metadata.name)
                                        pod.metadata.name = p.metadata.name
                                        index_pod = self.monitor.all_pods.getIndexPod(lambda x: x.metadata.name == obj.metadata.name)
                                        self.monitor.all_pods.items[index_pod] = pod
                                        break
                                    else:
                                        pass
                                self.monitor.wait_for_pod(pod)
                                pod_scheduled = False
                                while not pod_scheduled:
                                    try:
                                        resp = self.v1.read_namespaced_pod(pod.metadata.name, 'default')
                                    except ApiException as e:
                                        print("Exception when calling CoreV1Api->read_namespaced_pod: %s\n" % e)
                                    if resp.spec.node_name is not None or resp.spec.node_name != 'None':
                                        pod_scheduled = True
                                if self.monitor.all_pods.isPodList(lambda x: x.metadata.name == pod.metadata.name):
                                    new_pod = self.monitor.all_pods.getPod(lambda x: x.metadata.name == pod.metadata.name)
                                    new_pod.spec.node_name = resp.spec.node_name
                                    index_pod = self.monitor.all_pods.getIndexPod(lambda x: x.metadata.name == new_pod.metadata.name)
                                    self.monitor.all_pods.items[index_pod] = new_pod
                                    if obj.event == "service":
                                        serv = self.monitor.all_services.getService(lambda x: x.id_ == obj.service_id)
                                        vnf = serv.vnfunctions.getVNF(lambda x: x.id_ == obj.id)
                                        vnf.starting_time = datetime.now()
                                        vnf.waiting_time = abs((vnf.starting_time - obj.service_arrival_time).seconds)
                                        vnf.in_node = resp.spec.node_name
                                        self.monitor.scheduled_VNFs += 1
                                        if vnf.id_ == serv.vnfunctions.items[0].id_:
                                            serv.waiting_time_first_VNF = vnf.waiting_time
                                        vnf_index = serv.vnfunctions.getIndexVNF(lambda x: x.id_ == vnf.id_)
                                        serv.vnfunctions.items[vnf_index] = vnf
                                        all_VNF_scheduled = serv.vnfunctions.areAllVNFScheduled(lambda x: x.in_node != None)
                                        if all_VNF_scheduled:
                                            self.monitor.scheduled_services += 1
                                        index_serv = self.monitor.all_services.getIndexService(lambda x: x.id_ == serv.id_)
                                        self.monitor.all_services.items[index_serv] = serv
                                        self.update_result_files()
                                        self.monitor.update_events_file(obj.event, vnf.name, vnf.id_, serv.id_, serv.name, vnf.service_arrival_time, None, vnf.r_rate, vnf.running_time, vnf.deadline, vnf.priority, vnf.waiting_time, vnf.starting_time, None, None, None, vnf.in_node)

                                    elif obj.event == "task":
                                        task = self.monitor.all_tasks.getTask(lambda x: x.id_ == obj.id)
                                        task.starting_time = datetime.now()
                                        task.waiting_time = abs((task.starting_time - obj.task_arrival_time).seconds)
                                        task.in_node = resp.spec.node_name
                                        self.monitor.scheduled_tasks += 1
                                        task_index = self.monitor.all_tasks.getIndexTask(lambda x: x.id_ == task.id_)
                                        self.monitor.all_tasks.items[task_index] = task
                                        self.update_result_files()
                                        self.monitor.update_events_file(obj.event, task.name, task.id_, None, None, None, task.task_arrival_time, task.r_rate, task.running_time, task.deadline, task.priority, task.waiting_time, task.starting_time, None, None, None, task.in_node)

                                    else:
                                        print('Event could not be updated')
                        else:
                            print('Pod cannot be scheduled..')
                    else:
                        if self.monitor.all_pods.isPodList(lambda x: x.metadata.name == obj.metadata.name):
                            new_node = self.choose_node(obj)
                            if new_node is not None:
                                self.bind_to_node(obj.metadata.name, new_node.metadata.name)
                                """
                                without this cluster for 2nd and next Pods in deployment looks the same,
                                so all Pods from deployment are placed on the same Node, we want to avoid this
                                block scheduling thread until newly created Pod is ready
                                """
                                self.monitor.wait_for_pod(obj)
                                if self.monitor.all_pods.isPodList(lambda x: x.metadata.name == obj.metadata.name):
                                    pod = self.monitor.all_pods.getPod(lambda x: x.metadata.name == obj.metadata.name)
                                    pod.spec.node_name = new_node.metadata.name
                                    index_pod = self.monitor.all_pods.getIndexPod(lambda x: x.metadata.name == pod.metadata.name)
                                    self.monitor.all_pods.items[index_pod] = pod
                                    if obj.event == "service":
                                        serv = self.monitor.all_services.getService(lambda x: x.id_ == obj.service_id)
                                        vnf = serv.vnfunctions.getVNF(lambda x: x.id_ == obj.id)
                                        vnf.starting_time = datetime.now()
                                        vnf.waiting_time = abs((vnf.starting_time - obj.service_arrival_time).seconds)
                                        vnf.in_node = new_node.metadata.name
                                        self.monitor.scheduled_VNFs += 1
                                        if vnf.id_ == serv.vnfunctions.items[0].id_:
                                            serv.waiting_time_first_VNF = vnf.waiting_time
                                        vnf_index = serv.vnfunctions.getIndexVNF(lambda x: x.id_ == vnf.id_)
                                        serv.vnfunctions.items[vnf_index] = vnf
                                        all_VNF_scheduled = serv.vnfunctions.areAllVNFScheduled(lambda x: x.in_node != None)
                                        if all_VNF_scheduled:
                                            self.monitor.scheduled_services += 1

                                            # Notify service deployment to the GC
                                            # if  serv.multideployment:
                                            #     data = {}
                                            #     data["Identificador"] = serv.name
                                            #     data["NodeId"] = ""
                                            #     data["TerminationPointId"] = "0"
                                            #     data["LinkId"] = "0"
                                            #     data["SourceNodeTp"] = ""
                                            #     data["DestinationNodeTp"] = "0"
                                            #     data["SourceNode"] = str(True)
                                            #     data["DestinationNode"] = self.localdds
                                                
                                            #     self.dds.lock.acquire()
                                            #     writer = self.dds.connector.get_output("kubernetes-control-plane1-pub::kubernetes-control-plane1-dw")
                                            #     writer.instance.set_dictionary(data)
                                            #     dt = int(datetime.now().timestamp() * 1000000000)
                                            #     writer.write(source_timestamp=dt)
                                            #     self.dds.lock.release()
                                        
                                        index_serv = self.monitor.all_services.getIndexService(lambda x: x.id_ == serv.id_)
                                        self.monitor.all_services.items[index_serv] = serv
                                        self.update_result_files()
                                        self.monitor.update_events_file(obj.event, vnf.name, vnf.id_, serv.id_, serv.name, vnf.service_arrival_time, None, vnf.r_rate, vnf.running_time, vnf.deadline, vnf.priority, vnf.waiting_time, vnf.starting_time, None, None, None, vnf.in_node)

                                    elif obj.event == "task":
                                        task = self.monitor.all_tasks.getTask(lambda x: x.id_ == obj.id)
                                        task.starting_time = datetime.now()
                                        task.waiting_time = abs((task.starting_time - obj.task_arrival_time).seconds)
                                        task.in_node = new_node.metadata.name
                                        self.monitor.scheduled_tasks += 1
                                        task_index = self.monitor.all_tasks.getIndexTask(lambda x: x.id_ == task.id_)
                                        self.monitor.all_tasks.items[task_index] = task
                                        self.update_result_files()
                                        self.monitor.update_events_file(obj.event, task.name, task.id_, None, None, None, task.task_arrival_time, task.r_rate, task.running_time, task.deadline, task.priority, task.waiting_time, task.starting_time, None, None, None, task.in_node)

                                    else:
                                        print('Event could not be updated')

                        else:
                            print('Pod cannot be scheduled..')
                            """
                            when Pod cannot be scheduled it is being deleted and after
                            couple seconds new Pod is being created and another attempt
                            of scheduling this Pod is being made
                            """
                except Exception as e:
                    print(str(e))

    def update_GC(self):
        """
        Method to update the GC by sending it
        the nodes status regarding CPU and SOC
        """
        while True:
            try:
                self.monitor.update_nodes()
                for node in self.monitor.all_nodes.items:
                    data = {}
                    data["Identificador"] = "Node_Status"
                    data["NodeId"] = node.metadata.name
                    data["TerminationPointId"] = str(node.usage['cpu'])
                    data["LinkId"] = str(node.SoC)
                    if node.pods.anyPodRunningVNF(lambda x: x.metadata.name.startswith('vnf')):
                        data["SourceNode"] = "1"
                    else:
                        data["SourceNode"] = "0"
                    data["DestinationNode"] = self.localdds

                    self.dds.lock.acquire()
                    writer = self.dds.connector.get_output("kubernetes-control-plane1-pub::kubernetes-control-plane1-dw")
                    writer.instance.set_dictionary(data)
                    dt = int(datetime.now().timestamp() * 1000000000)
                    writer.write(source_timestamp=dt)
                    self.dds.lock.release()

                time.sleep(self.time_interval_multideployment)
            except Exception as e:
                print(str(e))

    def is_pod_deployed(self, pod):
        self.monitor.wait_for_pod(pod)
        pod_scheduled = False
        while not pod_scheduled:
            try:
                resp = self.v1.read_namespaced_pod(pod.metadata.name, 'default')
                if resp.spec.node_name is not None or resp.spec.node_name != 'None':
                    pod_scheduled = True
                    pod.spec.node_name = resp.spec.node_name
                    index_pod = self.monitor.all_pods.getIndexPod(lambda x: x.metadata.name == pod.metadata.name)
                    if index_pod is not None:
                        self.monitor.all_pods.items[index_pod] = pod
                    else:
                        self.monitor.all_pods.items.append(pod)
            except ApiException as e:
                print("Exception when calling CoreV1Api->read_namespaced_pod: %s\n" % e)
                break

        return pod_scheduled

    def choose_node(self, pod):
        """
        Method that brings together all methods
        responsible for choosing best Node for a Pod
        :param pod.Pod pod: Pod to be scheduled
        :return node.Node: return best selected Node for Pod,
            None if Pod cannot be scheduled
        """
        possible_nodes = self.filter_nodes(pod)

        selected_node = self.score_nodes(pod, possible_nodes)

        if selected_node is None:
            print('No node was selected')
            
        return selected_node

    def filter_nodes(self, pod):
        """
        Filter Nodes in self.monitor.all_nodes
        which can run selected Pod
        :param pod.Pod pod: Pod to be scheduled
        :return node.NodeList: List of Node which
            satisfy Pod requirements
        """
        return_node_list = NodeList()
        
        if pod.spec.node_name is not None:
            for node in self.monitor.all_nodes.items:
                if pod.spec.node_name == node.metadata.name and node.spec.unschedulable is not True:
                    return_node_list.items.append(node)
        else:
            self.monitor.update_nodes()

            for node in self.monitor.all_nodes.items:
                if pod.scheduling_criteria == 0 and int(float(node.SoC)) > settings.MIN_SOC_NODES and pod.parse_usage_data(pod.spec.containers[0].resources.requests['cpu'], DataType.CPU) < (self.monitor.max_cpu_per_node - node.usage['cpu']) and node.spec.unschedulable is not True:
                     return_node_list.items.append(node)
                elif pod.scheduling_criteria == 1 and int(float(node.SoC)) > settings.MIN_SOC_NODES and node.spec.unschedulable is not True:
                    return_node_list.items.append(node)
                elif pod.scheduling_criteria == 2 and pod.parse_usage_data(pod.spec.containers[0].resources.requests['cpu'], DataType.CPU) < (self.monitor.max_cpu_per_node - node.usage['cpu'] - 250) and node.spec.unschedulable is not True:
                    return_node_list.items.append(node)
                elif pod.scheduling_criteria == 3 and int(pod.demanded_processing) < node.proc_capacity and node.spec.unschedulable is not True:
                    return_node_list.items.append(node)
                else: 
                    continue

            if len(return_node_list.items) > 1:
                if return_node_list.isNodeList(lambda x: x.metadata.name == settings.MASTER_NAME):
                    tmp_node = return_node_list.getNode(lambda x: x.metadata.name == settings.MASTER_NAME)
                    return_node_list.items.remove(tmp_node)

            if len(return_node_list.items) == 0 or (len(return_node_list.items) == 1 and return_node_list.isNodeList(lambda x: x.metadata.name == settings.MASTER_NAME) and int(pod.running_time) == 0):
                print('None node can meet the %s pod requirements' % pod.metadata.name)
                if len(return_node_list.items) == 1 and return_node_list.isNodeList(lambda x: x.metadata.name == settings.MASTER_NAME):
                    tmp_node = return_node_list.getNode(lambda x: x.metadata.name == settings.MASTER_NAME)
                    return_node_list.items.remove(tmp_node)    
                if pod.event == "service":
                    if self.monitor.all_services.isServiceList(lambda x: x.id_ == pod.service_id):
                        serv = self.monitor.all_services.getService(lambda x: x.id_ == pod.service_id)
                        numberVNFs = len(serv.vnfunctions.items)
                        for i in serv.vnfunctions.items:
                            if int(serv.running_time) > 0:
                                self.monitor.delete_job(i.name)
                            else:
                                self.monitor.delete_deployment(i.name)

                        self.monitor.status_lock.acquire(blocking=True)

                        self.monitor.all_services.items.remove(serv)
                        for i in self.monitor.all_pods.items:
                            if i.service_id == serv.id_:
                                self.monitor.all_pods.items.remove(i)
                                print('Pod %s deleted' % i.metadata.name)

                        provi_list = list(self.priority_list)
                        for i in provi_list:
                            if i.service_id == serv.id_:
                                provi_list.remove(i)
                        self.priority_list = provi_list.copy()

                        self.monitor.status_lock.release()

                        vnf_serv_scheduled = 0
                        for i in serv.vnfunctions.items:
                            if i.in_node is not None:
                                vnf_serv_scheduled += 1

                        self.monitor.rejected_services += 1
                        self.monitor.rejected_VNFs += numberVNFs - vnf_serv_scheduled

                        # Notify service rejection to the GC
                        if  serv.multideployment:
                            data = {}
                            data["Identificador"] = serv.name
                            data["NodeId"] = ""
                            data["TerminationPointId"] = "0"
                            data["LinkId"] = "0"
                            data["SourceNodeTp"] = ""
                            data["DestinationNodeTp"] = "-1"
                            data["SourceNode"] = str(True)
                            data["DestinationNode"] = self.localdds

                            self.dds.lock.acquire()
                            writer = self.dds.connector.get_output("kubernetes-control-plane1-pub::kubernetes-control-plane1-dw")
                            writer.instance.set_dictionary(data)
                            dt = int(datetime.now().timestamp() * 1000000000)
                            writer.write(source_timestamp=dt)
                            self.dds.lock.release()

                        self.update_result_files()

                elif pod.event == "task":
                    task = self.monitor.all_tasks.getTask(lambda x: x.id_ == pod.id)
                    self.monitor.delete_job(task.name)

                    self.monitor.status_lock.acquire(blocking=True)

                    self.monitor.all_tasks.items.remove(task)
                    self.monitor.all_pods.items.remove(pod)
                    print('Pod %s deleted' % pod.metadata.name)

                    self.monitor.status_lock.release()

                    self.monitor.rejected_tasks += 1

                    self.update_result_files()
                else:
                    print('Error! Some event must be detected')

        return return_node_list

    def score_nodes(self, pod, node_list):
        """
        Score Nodes passed in node_list to choose the best one
        :param pod.Pod pod: Pod to be scheduled
        :param node.NodeList node_list: Nodes which meet Pod
            requirements
        :return node.Node: return Node which got highest score
            for Pod passed as pod, None if any node cannot be
            selected
        """
        best_node = None

        if len(node_list.items) == 0:
            pass

        elif len(node_list.items) == 1:
            best_node = node_list.items[0]

        else:
            best_node = node_list.items[0]

            for node in node_list.items:

                if pod.scheduling_criteria == 0: # scheduling criteria is a mixed relation between CPU and SOC estimation
                    if self.pred_model is not None:
                        if best_node.score == 0.0:
                            print('Calculating score for best node')
                            best_node.SoC_pred = self.predict_SOC(best_node, pod)
                            e_cpu = float(best_node.usage['cpu']) + float(pod.parse_usage_data(pod.spec.containers[0].resources.requests['cpu'], DataType.CPU))
                            best_node.score = self.calculate_score(best_node.SoC_pred, e_cpu)
                            print(best_node.score)
                        print('Calculating score for current node')
                        node.SoC_pred = self.predict_SOC(node, pod)
                        e_cpu = float(node.usage['cpu']) + float(pod.parse_usage_data(pod.spec.containers[0].resources.requests['cpu'], DataType.CPU))
                        node.score = self.calculate_score(node.SoC_pred, e_cpu)
                        print(node.metadata.name, node.score)
                        if node.score > best_node.score:
                            best_node = node
                    else: 
                        if best_node.score == 0.0:
                            print('Calculating score best node')
                            e_cpu = float(best_node.usage['cpu']) + float(pod.parse_usage_data(pod.spec.containers[0].resources.requests['cpu'], DataType.CPU))
                            best_node.score = self.calculate_score(best_node.SoC, e_cpu)
                            print(best_node.score)
                        print('Calculating score for current node')
                        e_cpu = float(node.usage['cpu']) + float(pod.parse_usage_data(pod.spec.containers[0].resources.requests['cpu'], DataType.CPU))
                        node.score = self.calculate_score(node.SoC, e_cpu)
                        print(node.metadata.name, node.score)
                        if node.score > best_node.score:
                            best_node = node

                elif pod.scheduling_criteria == 1: # scheduling criteria takes into account the SOC estimation
                    if self.pred_model is not None:
                        if best_node.SoC_pred == '':
                            print('Predicting SOC for best node')
                            best_node.SoC_pred = self.predict_SOC(best_node, pod)

                        print('Predicting SOC for current node')
                        node.SoC_pred = self.predict_SOC(node, pod)
                        if float(node.SoC_pred) < float(best_node.SoC_pred):
                            best_node = node
                    else:
                        if float(node.SoC) < float(best_node.SoC):
                            best_node = node
                            
                elif pod.scheduling_criteria == 2: # scheduling criteria takes into account the CPU usage
                    if (self.monitor.max_cpu_per_node - node.usage['cpu']) > (self.monitor.max_cpu_per_node - best_node.usage['cpu']):
                        best_node = node

                elif pod.scheduling_criteria == 3: # scheduling criteria takes into account the best processing times in the nodes
                    if node.proc_capacity > best_node.proc_capacity:
                        best_node = node

                else:
                    print('None running scoring process was selected')
                    pass
        
        if best_node is not None:
            print('Selected node:')
            print(best_node.metadata.name, best_node.usage, best_node.score)
        return best_node

    def calculate_score(self, soc, cpu):
        """
        Calculate score for a Node using defined formula
        :return int: Node score
        """
        score = (settings.alfa_1 * (float(soc) / 100)) + ((1 - settings.alfa_1) * (1 - (cpu / self.monitor.max_cpu_per_node))) 
        return score

    def predict_SOC(self, node, pod):
        '''
        Predict the SOC of the node using the trained model
        '''
        current_soc_pred = None
        count = 0

        if str(node.metadata.name).endswith('control-plane'):

            for pod_ in self.monitor.all_pods.items:
                if pod_.status.phase == 'Running':
                    count += 1

            cpu = float(node.usage['cpu']) + float(pod.parse_usage_data(pod.spec.containers[0].resources.requests['cpu'], DataType.CPU))
            net_in = float(node.net_in_pkt) + float(node.net_in_pkt)/float(count)

            data = {'CPU': cpu,
                    'Net_In_Pkt': net_in,
                    'Worker1': 0,
                    'Worker2': 0,
                    'Worker3': 0}
            print(data)
            current_soc_pred = self.pred_model.predict(data, transform=True)
            print('Prediction for node ' + node.metadata.name + ' is ' + str(current_soc_pred[0]))
        else:
            dum1 = 0
            dum2 = 0
            dum3 = 0
            if str(node.metadata.name).endswith('worker1'):
                dum1 = 1
            if str(node.metadata.name).endswith('worker2'):
                dum2 = 1
            if str(node.metadata.name).endswith('worker3'):
                dum3 = 1

            cpu = float(node.usage['cpu']) + float(pod.parse_usage_data(pod.spec.containers[0].resources.requests['cpu'], DataType.CPU))

            data = {'CPU': cpu,
                    'Net_In_Pkt': 0.0,
                    'Worker1': dum1,
                    'Worker2': dum2,
                    'Worker3': dum3}
            print(data)
            current_soc_pred = self.pred_model.predict(data, transform=True)
            print('Prediction for node ' + node.metadata.name + ' is ' + str(current_soc_pred[0]))

        return str(current_soc_pred[0])

    def train_model(self, dataset):
        '''
        Train the regression model using the created training dataset by the monitoring process
        '''
        ind = 0
        for j in self.monitor.all_nodes.items:
            for i in range(len(dataset['SoC_real'])):
                if dataset['Node'][i] == j.metadata.name and not pd.isna(dataset['SoC_real'][i]):
                    ind = i
                    break
            for i in range(len(dataset['SoC_real'])):
                if dataset['Node'][i] == j.metadata.name and pd.isna(dataset['SoC_real'][i]):
                    dataset['SoC_real'][i] = dataset['SoC_real'][ind]

        dataset = dataset[dataset['CPU'] != 0]

        dataset['SoC_real'] = dataset['SoC_real'].apply(pd.to_numeric, errors='coerce')
        dataset['CPU'] = dataset['CPU'].apply(pd.to_numeric, errors='coerce')
        dataset['Net_In_Pkt'] = dataset['Net_In_Pkt'].apply(pd.to_numeric, errors='coerce')

        dum = pd.get_dummies(dataset.Node, prefix='Node').iloc[:, 1:]
        dum = pd.concat([dataset, dum], axis = 1)
        dum.rename(columns={'Node_kubernetes-worker1': 'Worker1'}, inplace=True)
        dum.rename(columns={'Node_kubernetes-worker2': 'Worker2'}, inplace=True)
        dum.rename(columns={'Node_kubernetes-worker3': 'Worker3'}, inplace=True)

        ml_model = smf.ols(formula='SoC_real ~ 1 + CPU + Net_In_Pkt + I(CPU ** 2.0)*(Worker1 + Worker2 + Worker3) + Worker1 + Worker2 + Worker3', data=dum).fit()

        return ml_model

    def update_model(self, dataset):
        '''
        Update the model parameters when the RMSE is above a predifined threshold
        '''
        ml_model = smf.ols(formula='SoC_real ~ 1 + CPU + Net_In_Pkt + I(CPU ** 2.0)*(Worker1 + Worker2 + Worker3) + Worker1 + Worker2 + Worker3', data=dataset).fit()
        return ml_model

    def check_model(self):
        '''
        Check the model parameters and calls the update_model method if the RMSE is above a predifined threshold
        '''
        initial_flag = True

        while True:
            if not initial_flag:
                if self.pred_model is None:
                    print('Initial training of the SOC estimation model')
                    self.monitor.status_lock.acquire(blocking=True)
                    self.pred_model = self.train_model(self.monitor.dataset_train)
                    self.monitor.dataset_train.to_csv('Predictions_values.csv', index=False, mode='a')
                    self.monitor.status_lock.release()
                    self.monitor.restart_dataset_train()
                else:
                    print('Checking SOC estimation model')
                    self.monitor.status_lock.acquire(blocking=True)
                    dataset = self.monitor.dataset_train.copy()
                    dataset1 = self.monitor.dataset_train.copy()

                    ind = 0
                    for j in self.monitor.all_nodes.items:
                        for i in range(len(dataset['SoC_real'])):
                            if dataset['Node'][i] == j.metadata.name and not pd.isna(dataset['SoC_real'][i]):
                                ind = i
                                break
                        for i in range(len(dataset['SoC_real'])):
                            if dataset['Node'][i] == j.metadata.name and pd.isna(dataset['SoC_real'][i]):
                                dataset['SoC_real'][i] = dataset['SoC_real'][ind]

                    dataset = dataset[dataset['CPU'] != 0]
                    dataset['SoC_real'] = dataset['SoC_real'].apply(pd.to_numeric, errors='coerce')
                    dataset['CPU'] = dataset['CPU'].apply(pd.to_numeric, errors='coerce')
                    dataset['Net_In_Pkt'] = dataset['Net_In_Pkt'].apply(pd.to_numeric, errors='coerce')

                    isnasoc = np.any(np.isnan(dataset['SoC_real']))
                    if isnasoc:
                        nans = np.where(np.isnan(dataset['SoC_real']))
                        for i in nans[0]:
                            ind = i + 4
                            ori_data = dataset1['SoC_real'][ind]
                            ori_data = str(ori_data)[:6]
                            dataset['SoC_real'][ind] = float(ori_data)

                    isnacpu = np.any(np.isnan(dataset['CPU']))
                    if isnacpu:
                        nans = np.where(np.isnan(dataset['CPU']))
                        for i in nans[0]:
                            ind = i + 4
                            ori_data = dataset1['CPU'][ind]
                            ori_data = str(ori_data)[:8]
                            dataset['CPU'][ind] = float(ori_data)

                    dum = pd.get_dummies(dataset.Node, prefix='Node').iloc[:, 1:]
                    dum = pd.concat([dataset, dum], axis = 1)
                    dum.rename(columns={'Node_kubernetes-worker1': 'Worker1'}, inplace=True)
                    dum.rename(columns={'Node_kubernetes-worker2': 'Worker2'}, inplace=True)
                    dum.rename(columns={'Node_kubernetes-worker3': 'Worker3'}, inplace=True)

                    SOC_pred = self.pred_model.predict(dum, transform=True)
                    SOC_pred = SOC_pred.round(4)
                    dataset['SoC_pred'] = SOC_pred
                    mae = metrics.mean_absolute_error(dataset['SoC_real'], SOC_pred)
                    mse = metrics.mean_squared_error(dataset['SoC_real'], SOC_pred)
                    rmse = np.sqrt(metrics.mean_squared_error(dataset['SoC_real'], SOC_pred))

                    self.update_predictions_file(mae, mse, rmse)

                    self.monitor.status_lock.release()

                    if rmse > settings.RMSE:
                        print('Updating predictors model')
                        self.pred_model = self.update_model(dum)
                        SOC_pred_upd = self.pred_model.predict(dum, transform=True)
                        dataset['SoC_pred_upd'] = SOC_pred_upd
                        mae = metrics.mean_absolute_error(dataset['SoC_real'], SOC_pred_upd)
                        mse = metrics.mean_squared_error(dataset['SoC_real'], SOC_pred_upd)
                        rmse = np.sqrt(metrics.mean_squared_error(dataset['SoC_real'], SOC_pred_upd))
                        self.update_predictions_file(mae, mse, rmse)
                        dataset.to_csv('Predictions_values.csv', index=False, header=False, mode='a')
                        self.monitor.restart_dataset_train()
                    
            initial_flag = False
            time.sleep(settings.ESTIMATION_CHECKING_TIME)

    def bind_to_node(self, pod_name, node_name, namespace='default'):
        """
        Bind Pod to a Node
        :param str pod_name: pod name which we are binding
        :param str node_name: node name which pod has to be binded
        :param str namespace: namespace of pod
        :return: True if pod was bound successfully, False otherwise
        """
        target = client.V1ObjectReference()
        target.kind = "Node"
        target.api_version = "v1"
        target.name = node_name

        meta = client.V1ObjectMeta()
        meta.name = pod_name
        body = client.V1Binding(target=target)
        body.target = target
        body.metadata = meta
        try:
            self.v1.create_namespaced_binding(namespace, body)
            return True
        except Exception as e:
            """
            create_namespaced_binding() throws exception:
            Invalid value for `target`, must not be `None`
            or
            despite the fact this exception is being thrown,
            Pod is bound to a Node and Pod is running
            """
            print('exception' + str(e))
            return False

    def create_deployment(self, event_name, node_name, cpu_requested, namespace='default'):
        """
        Create deployment on an specific Node
        :param str event_name: event name to deploy
        :param str node_name: node name to deploy the event
        :param str cpu_requested: cpu requested by the event
        :param str namespace: namespace of event
        :return: True if event was deployed successfully, False otherwise
        """
        # Configureate Pod template container
        container = client.V1Container(
            name=event_name,
            image="alexllor1991/complexities:latest",
            ports=[client.V1ContainerPort(container_port=8080)],
            resources=client.V1ResourceRequirements(
                requests={"cpu": cpu_requested + "m", "memory": "325Mi"}
            ),
            volume_mounts=[client.V1VolumeMount(name="tz-paris", mount_path='/etc/localtime')],
        )

        volume = client.V1Volume(name='tz-paris', host_path=client.V1HostPathVolumeSource(path="/etc/localtime"))

        toleration1 = client.V1Toleration(effect="NoSchedule", key="node-role.kubernetes.io/master", operator="Exists")
        toleration2 = client.V1Toleration(effect="NoSchedule", key="node-role.kubernetes.io/control-plane", operator="Exists")

        # Create and configure a spec section
        template = client.V1PodTemplateSpec(
            metadata=client.V1ObjectMeta(labels={"app": event_name}),
            spec=client.V1PodSpec(containers=[container], volumes=[volume], image_pull_secrets=[client.V1LocalObjectReference('regcred')], node_selector={"kubernetes.io/hostname":node_name}, tolerations=[toleration1, toleration2]),
        )

        # Create the specification of deployment
        spec = client.V1DeploymentSpec(
            replicas=1, template=template, strategy={"type":"Recreate"}, selector={
                "matchLabels":
                {"app": event_name}})

        deployment = client.V1Deployment(
            api_version="apps/v1",
            kind="Deployment",
            metadata=client.V1ObjectMeta(name=event_name),
            spec=spec,
        )

        try:
            self.apps_v1.create_namespaced_deployment(namespace, deployment)
            return True
        except Exception as e:
            print('exeception' + str(e))
            return False

    def create_job(self, event_name, node_name, cpu_requested, namespace='default'):
        """
        Create job on an specific Node
        :param str event_name: event name to deploy
        :param str node_name: node name to deploy the event
        :param str cpu_requested: cpu requested by the event
        :param str namespace: namespace of event
        :return: True if event was deployed successfully, False otherwise
        """
        # Configureate Pod template container
        container = client.V1Container(
            name=event_name,
            image="alexllor1991/cpu_stress:latest",
            ports=[client.V1ContainerPort(container_port=8080)],
            resources=client.V1ResourceRequirements(
                requests={"cpu": cpu_requested + "m", "memory": "325Mi"}
            ),
            volume_mounts=[client.V1VolumeMount(name="tz-paris", mount_path='/etc/localtime')],
        )

        volume = client.V1Volume(name='tz-paris', host_path=client.V1HostPathVolumeSource(path="/etc/localtime"))

        toleration1 = client.V1Toleration(effect="NoSchedule", key="node-role.kubernetes.io/master", operator="Exists")
        toleration2 = client.V1Toleration(effect="NoSchedule", key="node-role.kubernetes.io/control-plane", operator="Exists")

        # Create and configure a spec section
        template = client.V1PodTemplateSpec(
            metadata=client.V1ObjectMeta(labels={"app": event_name}),
            spec=client.V1PodSpec(restart_policy="Never", containers=[container], volumes=[volume], image_pull_secrets=[client.V1LocalObjectReference('regcred')], node_selector={"kubernetes.io/hostname":node_name}, tolerations=[toleration1, toleration2]),
        )

        # Create the specification of job
        spec = client.V1JobSpec(
            template=template, ttl_seconds_after_finished=10)

        job = client.V1Job(
            api_version="batch/v1",
            kind="Job",
            metadata=client.V1ObjectMeta(name=event_name),
            spec=spec)

        try:
            self.batch_v1.create_namespaced_job(namespace, job)
            return True
        except Exception as e:
            print('exeception' + str(e))
            return False
        
    def pass_to_scheduler(self, name_, namespace_, scheduler_name_, event_):
        """
        Pass deployment to be scheduled by different scheduler
        :param str scheduler_name_: name of new scheduler, which will
            schedule this deployment
        :param str name_: name of deployment
        :param str namespace_: namespace of deployment
        :return str: return http response code
        """
        if event_ == 'deployment':
            obj_dep = self.apps_v1.read_namespaced_deployment(name_, namespace_, pretty='pretty_example')
            obj_dep.spec.template.spec.scheduler_name = scheduler_name_
            try:
                api_response = self.apps_v1.patch_namespaced_deployment(name_, namespace_, obj_dep)
            except ApiException as e:
                print("Exception when calling AppsV1Api->patch_namespaced_deployment: %s\n" % e)
        elif event_ == 'job':
            obj_job = self.batch_v1.read_namespaced_job(name_, namespace_)
            container = obj_job.spec.template.spec.containers
            volume = obj_job.spec.template.spec.volumes
            toleration = client.V1Toleration(key='node-role.kubernetes.io/master', effect='NoSchedule')
            template = client.V1PodTemplateSpec(metadata=client.V1ObjectMeta(labels={'app': name_}, annotations=obj_job.spec.template.metadata.annotations),
                                                spec=client.V1PodSpec(restart_policy="Never", containers=container, scheduler_name=scheduler_name_, volumes=volume, tolerations=[toleration]))
            spec = client.V1JobSpec(template=template, ttl_seconds_after_finished=10)
            job = client.V1Job(api_version='batch/v1', kind='Job', metadata=client.V1ObjectMeta(name=name_), spec=spec)
            try:
                api_response = self.batch_v1.delete_namespaced_job(name_, namespace_, body=client.V1DeleteOptions(propagation_policy='Foreground', grace_period_seconds=1))
            except ApiException as e:
                print("Exception when calling AppsV1Api->delete_namespaced_job: %s\n" % e)
            job_deleted = False
            while not job_deleted:
                found = False
                for j in self.batch_v1.list_job_for_all_namespaces().items:
                    if j.metadata.name == name_:
                        found = True
                if found:
                    pass
                else:
                    job_deleted = True
            try:
                api_response = self.batch_v1.create_namespaced_job(namespace_, job)
            except ApiException as e:
                print("Exception when calling AppsV1Api->create_namespaced_job: %s\n" % e)
        else:
            print('Error passing to native scheduler!!')

    def update_predictions_file(self, mae, mse, rmse):
        '''
        Saves the prediction results to Predictions_results.csv file
        '''
        with open('Predictions_results.csv', 'a', buffering=1) as csvfile1:
                writer1 = csv.DictWriter(csvfile1, fieldnames=self.fieldnames_prediction_results)
                writer1.writerow({'Timestamp': str(datetime.now()),
                                  'R_Squared': str(self.pred_model.rsquared),
                                  'MAE': str(mae),
                                  'MSE': str(mse),
                                  'RMSE': str(rmse),
                                  'Model': 'SOC={}+{}*CPU+{}*Net_In_Pkt+({}*Worker1+{}*Worker2+{}*Worker3)*{}*CPU^2+{}*Worker1+{}*Worker2+{}*Worker3'.format(self.pred_model.params[0], self.pred_model.params[1], self.pred_model.params[2], self.pred_model.params[7], self.pred_model.params[8], self.pred_model.params[9], self.pred_model.params[3], self.pred_model.params[4], self.pred_model.params[5], self.pred_model.params[6])})

def main():
    try:
        config.load_incluster_config()
    except:
        config.load_kube_config() 
    scheduler = Scheduler()
    scheduler.run()

if __name__ == '__main__':
    main()
