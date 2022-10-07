import time
import json
import os
import csv
import logging
from time import sleep
from datetime import datetime, timedelta
from threading import Thread, Lock
import multiprocessing
import socket
import psutil
import pandas as pd
import random

from kubernetes import client, config
from kubernetes.client.rest import ApiException
from kubernetes.client.models.v1_container_image import V1ContainerImage

import settings
from node import Node, NodeList
from pod import Pod, PodList, DataType
from service import ServiceList, TaskList, Service, VNFunction, Task

NUMBER_OF_RETRIES = 7

logging.basicConfig(filename=settings.LOG_FILE, level=logging.INFO, datefmt='%Y-%m-%d %H:%M:%S',
                    format='%(asctime)s:%(levelname)s:%(message)s')

pd.options.mode.chained_assignment = None

class ClusterMonitor:
    """
    Create full view of cluster, periodically update
    info about Pods runtime resources usage, use this
    as statistics, to use average value instead of
    instantaneous value
    """

    def __init__(self):
        self.status_lock = Lock()

        # time interval in seconds to update Pods statistics
        self.time_interval = settings.TIME_INTERVAL
        self.available_sensor = settings.SOC_SENSOR

        configuration = client.Configuration()
        self.v1 = client.CoreV1Api(client.ApiClient(configuration))
        self.batch_v1 = client.BatchV1Api(client.ApiClient(configuration))
        self.apps_v1 = client.AppsV1Api(client.ApiClient(configuration))

        self.all_pods = PodList()
        self.multideployed_pods = PodList()
        self.scheduled_running_pods = PodList()
        self.all_nodes = NodeList()
        self.all_services = ServiceList()
        self.all_tasks = TaskList()
        self.pods_not_to_garbage = []

        self.client_data = multiprocessing.Manager().dict()

        self.requested_services = 0
        self.scheduled_services = 0
        self.requested_VNFs = 0
        self.scheduled_VNFs = 0
        self.requested_tasks = 0
        self.scheduled_tasks = 0
        self.rejected_services = 0
        self.rejected_VNFs = 0
        self.rejected_tasks = 0
        self.deadline_violations = 0
        self.failed_tasks = 0
        self.failed_vnfs = 0

        self.initial_SOC = 100.0

        self.fieldnames_usage_node = ['Timestamp',
                                      'Node',
                                      'CPU',
                                      'Memory',
                                      'Proc_Capacity',
                                      'Net_In_Pkt',
                                      'SoC']

        self.fieldnames_predictions = ['Timestamp',
                                      'Node',
                                      'CPU',
                                      'Net_In_Pkt',
                                      'SoC_real',
                                      'SoC_pred',
                                      'SoC_pred_upd']

        self.fieldnames_events = ['Timestamp',
                                  'Event',
                                  'Name',
                                  'Id',
                                  'Service_Id',
                                  'Service_Name',
                                  'Service_arrival_time',
                                  'Task_arrival_time',
                                  'Demanded_rate',
                                  'Running_time',
                                  'Deadline',
                                  'Priority',
                                  'Waiting_time',
                                  'Starting_time',
                                  'Completion_time',
                                  'Execution_time',
                                  'Flow_time',
                                  'Assigned_node']

        self.dataset_train = pd.DataFrame(columns=self.fieldnames_predictions)

        self.max_cpu_per_node = settings.TOTAL_CPU_CLUSTER / settings.NUMBER_NODES_CLUSTER
        self.max_mem_per_node = settings.TOTAL_MEMORY_CLUSTER / settings.NUMBER_NODES_CLUSTER

    def print_nodes_stats(self):
        """
        Print node stats
        :return:
        """
        for node in self.all_nodes.items:
            print(node.metadata.name, node.usage)

    #function to receive SoC from a connected device
    def startReceivingSoC(self, connector, address):
        '''
        Receives the SOC information from each agent running in the nodes
        '''
        while True:

            SoC_str = connector.recv(1024).decode()

            if SoC_str != '':

                self.client_data[address] = {
                    "SoC": str(SoC_str)[:6],
                    "connector": connector
                }

            time.sleep(5)

    def update_nodes(self):
        """
        Makes request to API about Nodes in cluster,
        then starts to add rest of attributes
        :return:
        """
        self.status_lock.acquire(blocking=True)
        if not len(self.all_nodes.items) > 0:
            self.all_nodes = NodeList()
        
        def names(self, names):
            self._names = names
        V1ContainerImage.names = V1ContainerImage.names.setter(names)
        
        try:    
            if len(self.v1.list_node().items) != len(self.all_nodes.items):
                for node_ in self.v1.list_node().items:
                    node = Node(node_.metadata, node_.spec, node_.status)
                    if str(node_.metadata.name).endswith('control-plane') and settings.DEPLOY_IN_MASTER is not True:
                        node.spec.unschedulable = True
                        print ("Node: " + node_.metadata.name + "--> Unschedulable")
                    node.update_node(self.all_pods)
                    #node.update_node(self.multideployed_pods)
                    if self.available_sensor:
                        if len(self.client_data) > 0:
                            ip_s = ''
                            if str(node.metadata.name).endswith('control-plane'):
                                ip_s = str(node.spec.to_dict()['pod_cidr'][:-4]) + "1"
                            else:
                                ip_s = str(node.spec.to_dict()['pod_cidr'][:-3])
                                #ip_s = str(node_.status.addresses[0].to_dict()['address'])
                            if ip_s in self.client_data:
                                current_SoC = self.client_data[ip_s]['SoC']
                                node.SoC = str(current_SoC)[:6]
                    else:
                        persistent_consumption = 0.0
                        limited_consumption = 0.0
                        idle_consumption_worker = 0.0123 #0.0223 0.0323
                        idle_consumption_master = 0.0262 #0.0362 0.0462
                        for p in node.pods.items:
                            if p.metadata.name.startswith('vnf-') and p.metadata.annotations['Deployment_type'] == 'Persistent':
                                #pod_consumption = round(random.uniform(0.0218, 0.0364), 4) 
                                #pod_consumption = round(random.uniform(0.0021, 0.0041), 4)
                                #pod_consumption = round(random.uniform(0.0021, 0.0031), 4) 
                                pod_consumption = round(random.uniform(0.0001, 0.0011), 4) 
                                persistent_consumption = persistent_consumption + pod_consumption
                            if p.metadata.name.startswith('vnf-') and p.metadata.annotations['Deployment_type'] == 'Limited':
                                #pod_consumption = round(random.uniform(0.0218, 0.0364), 4)
                                #pod_consumption = round(random.uniform(0.0021, 0.0041), 4)
                                #pod_consumption = round(random.uniform(0.0021, 0.0031), 4) 
                                pod_consumption = round(random.uniform(0.0001, 0.0011), 4) 
                                limited_consumption = limited_consumption + pod_consumption
                        if str(node.metadata.name).endswith('control-plane'):
                            current_consumption = persistent_consumption + limited_consumption + idle_consumption_master
                            if node.SoC == '':
                                currentSoC = self.initial_SOC - current_consumption
                                node.SoC = str(currentSoC)
                            else:
                                currentSoC = float(node.SoC) - current_consumption
                                node.SoC = str(currentSoC)
                        else:
                            current_consumption = persistent_consumption + limited_consumption + idle_consumption_worker
                            if node.SoC == '':
                                currentSoC = self.initial_SOC - current_consumption
                                node.SoC = str(currentSoC)
                            else:
                                currentSoC = float(node.SoC) - current_consumption
                                node.SoC = str(currentSoC)
                    self.all_nodes.items.append(node)
                    #print(node.metadata.name, node.SoC)
            for node_ in self.all_nodes.items:
                node_.update_node(self.all_pods)
                #node_.update_node(self.multideployed_pods)
                if self.available_sensor:
                    if len(self.client_data) > 0:
                        ip_s = ''
                        if str(node_.metadata.name).endswith('control-plane'):
                            ip_s = str(node_.spec.to_dict()['pod_cidr'][:-4]) + "1"
                        else:
                            ip_s = str(node_.spec.to_dict()['pod_cidr'][:-3])
                            #ip_s = str(node_.status.addresses[0].to_dict()['address'])
                        if ip_s in self.client_data:
                            current_SoC = self.client_data[ip_s]['SoC']
                            node_.SoC = str(current_SoC)[:6]
                else:
                    persistent_consumption = 0.0
                    limited_consumption = 0.0
                    idle_consumption_worker = 0.0123 #0.0223 0.0323
                    idle_consumption_master = 0.0262 #0.0362 0.0462
                    for p in node_.pods.items:
                        if p.metadata.name.startswith('vnf-') and p.metadata.annotations['Deployment_type'] == "Persistent":
                            #pod_consumption = round(random.uniform(0.0218, 0.0364), 4)
                            #pod_consumption = round(random.uniform(0.0021, 0.0041), 4)
                            #pod_consumption = round(random.uniform(0.0021, 0.0031), 4) 
                            pod_consumption = round(random.uniform(0.0001, 0.0011), 4) 
                            persistent_consumption = persistent_consumption + pod_consumption
                        if p.metadata.name.startswith('vnf-') and p.metadata.annotations['Deployment_type'] == "Limited":
                            #pod_consumption = round(random.uniform(0.0218, 0.0364), 4)
                            #pod_consumption = round(random.uniform(0.0021, 0.0041), 4)
                            #pod_consumption = round(random.uniform(0.0021, 0.0031), 4)
                            pod_consumption = round(random.uniform(0.0001, 0.0011), 4)   
                            limited_consumption = limited_consumption + pod_consumption
                    if str(node_.metadata.name).endswith('control-plane'):
                        current_consumption = persistent_consumption + limited_consumption + idle_consumption_master
                        if node_.SoC == '':
                            currentSoC = self.initial_SOC - current_consumption
                            node_.SoC = str(currentSoC)
                        else:
                            currentSoC = float(node_.SoC) - current_consumption
                            node_.SoC = str(currentSoC)
                    else:
                        current_consumption = persistent_consumption + limited_consumption + idle_consumption_worker
                        if node_.SoC == '':
                            currentSoC = self.initial_SOC - current_consumption
                            node_.SoC = str(currentSoC)
                        else:
                            currentSoC = float(node_.SoC) - current_consumption
                            node_.SoC = str(currentSoC)

                if node_.usage['cpu'] >= self.max_cpu_per_node - 300 and node_.spec.unschedulable is not True:
                    node_.spec.unschedulable = True
                    print ("Node: " + node_.metadata.name + "--> Unschedulable")
                    body = {
                        "spec": {
                            "unschedulable": True
                        }
                    }
                    self.v1.patch_node(node_.metadata.name, body)
                if node_.usage['cpu'] < self.max_cpu_per_node - 300 and node_.spec.unschedulable is True:
                    if str(node_.metadata.name).endswith('control-plane') and settings.DEPLOY_IN_MASTER is not True:
                        node_.spec.unschedulable = True
                    else:
                        node_.spec.unschedulable = False
                    print ("Node: " + node_.metadata.name + "--> Schedulable")
                    body = {
                        "spec": {
                            "unschedulable": False
                        }
                    }
                    self.v1.patch_node(node_.metadata.name, body)

                index = self.all_nodes.getIndexNode(lambda x: x.metadata.name == node_.metadata.name)
                self.all_nodes.items[index] = node_
                #print(node_.metadata.name, node_.SoC)

            for node_ in self.v1.list_node().items:
                for condition in node_.status.conditions:
                    node = self.all_nodes.getNode(lambda x: x.metadata.name == node_.metadata.name)
                    if condition.type == "Ready" and condition.status != "True":
                        node.ready = "False"
                    else:
                        node.ready = "True"
                    index = self.all_nodes.getIndexNode(lambda x: x.metadata.name == node.metadata.name)
                    self.all_nodes.items[index] = node

            self.status_lock.release()
        except ApiException as e:
            print("Error: in list_node", e)

    def monitor_runner(self):
        """
        Run Pod monitor
        :return:
        """
        print('Monitor runner started')
        print('Maximum cpu capacity per node: ' + str(self.max_cpu_per_node))
        print('Maximum memory capacity per node: ' + str(self.max_mem_per_node))
        while True:
            self.update_pods()
            time.sleep(self.time_interval)

    def monitor_SoC_nodes(self):
        """
        Monitor Nodes State of Charge (%)
        :return:
        """
        master_server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        master_server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

        print("Socket created successfully")

        master_server.bind(('', 8080))

        #listening requests
        master_server.listen(10)
        print("Server started...\n")

        #making connections
        print("Starting to make connections...\n")

        while True:
            master_worker_connector, addr = master_server.accept()
            device_address = str(addr[0]) 

            print(device_address + " got connected successfully")

            current_thread = Thread(target=self.startReceivingSoC, daemon=False, args=(master_worker_connector, device_address, ))
            current_thread.start()

    def wait_for_pod(self, new_pod):
        """
        Wait for Pod to be ready - got metrics from
        metrics server
        :param pod.Pod new_pod: Pod to wait for
        :return:
        """
        retries = 0
        retries_not_ready = 0

        while True:
            found = False

            self.status_lock.acquire(blocking=True)

            for pod in self.all_pods.items:
                if new_pod.metadata.name == pod.metadata.name:
                    found = True
                    break

            self.status_lock.release()

            if found:
                #print('Waiting for pod %s' % new_pod.metadata.name)
                val = new_pod.fetch_usage()

                if val == 0:
                    break
                else:

                    retries_not_ready += 1

                    if retries_not_ready == NUMBER_OF_RETRIES:
                        tmp_mem = 0.0
                        tmp_cpu = 0.0
                        found = False
                        tmp_cont = None

                        for cont in new_pod.spec.containers:
                            if cont.resources.requests is not None:
                                tmp_cont = cont
                                found = True
                                break

                        if found:
                            if 'cpu' in cont.resources.requests:
                                tmp_cpu += float(new_pod.parse_usage_data(cont.resources.requests['cpu'], DataType.CPU))
                            if 'memory' in cont.resources.requests:
                                tmp_mem += float(new_pod.parse_usage_data(cont.resources.requests['memory'], DataType.MEMORY))
                                
                        if len(new_pod.usage) > settings.LIMIT_OF_RECORDS:
                            new_pod.usage.pop(0)
                        
                        new_pod.usage.append({'cpu': tmp_cpu, 'memory': tmp_mem})

                        break

            else:

                retries += 1

                if retries == NUMBER_OF_RETRIES:
                    break

            sleep(1)

    def wait_for_multideployment_pod(self, new_pod):
        """
        Wait for Pod to be ready - got metrics from
        metrics server
        :param pod.Pod new_pod: Pod to wait for
        :return:
        """
        retries = 0
        retries_not_ready = 0

        while True:
            found = False

            self.status_lock.acquire(blocking=True)

            for pod in self.multideployed_pods.items:
                if new_pod.metadata.name == pod.metadata.name:
                    found = True
                    break

            self.status_lock.release()

            if found:
                #print('Waiting for pod %s' % new_pod.metadata.name)
                val = new_pod.fetch_usage()

                if val == 0:
                    break
                else:

                    retries_not_ready += 1

                    if retries_not_ready == NUMBER_OF_RETRIES:
                        tmp_mem = 0.0
                        tmp_cpu = 0.0
                        found = False
                        tmp_cont = None

                        for cont in new_pod.spec.containers:
                            if cont.resources.requests is not None:
                                tmp_cont = cont
                                found = True
                                break

                        if found:
                            if 'cpu' in cont.resources.requests:
                                tmp_cpu += float(new_pod.parse_usage_data(cont.resources.requests['cpu'], DataType.CPU))
                            if 'memory' in cont.resources.requests:
                                tmp_mem += float(new_pod.parse_usage_data(cont.resources.requests['memory'], DataType.MEMORY))
                                
                        if len(new_pod.usage) > settings.LIMIT_OF_RECORDS:
                            new_pod.usage.pop(0)
                        
                        new_pod.usage.append({'cpu': tmp_cpu, 'memory': tmp_mem})

                        break

            else:

                retries += 1

                if retries == NUMBER_OF_RETRIES:
                    break

            sleep(1)

    def update_pods(self):
        """
        Update all Pods in cluster, if Pod exists add usage statistics
        to self.monitor_pods_data
        :return:
        """
        self.status_lock.acquire(blocking=True)

        # set all current pods as inactive
        for pod in self.all_pods.items:
            pod.is_alive = False

        for pod_ in self.v1.list_pod_for_all_namespaces().items:

            skip = False

            if pod_.status.phase == 'Running':
                for pod in self.all_pods.items:
                    if pod_.metadata.name == pod.metadata.name:
                        pod.status = pod_.status
                        index_pod = self.all_pods.getIndexPod(lambda x: x.metadata.name == pod.metadata.name)
                        self.all_pods.items[index_pod] = pod
                        # found in collection, so update its usage
                        skip = True  # skip creating new Pod
                        pod.is_alive = True

                        res = pod.fetch_usage()

                        if res != 0:
                            tmp_mem = 0.0
                            tmp_cpu = 0.0
                            found = False
                            tmp_cont = None

                            for cont in pod.spec.containers:
                                if cont.resources.requests is not None:
                                    tmp_cont = cont
                                    found = True
                                    break

                            if found:
                                if 'cpu' in cont.resources.requests:
                                    tmp_cpu += float(pod.parse_usage_data(cont.resources.requests['cpu'], DataType.CPU))
                                if 'memory' in cont.resources.requests:
                                    tmp_mem += float(pod.parse_usage_data(cont.resources.requests['memory'], DataType.MEMORY))
                                
                            if len(pod.usage) > settings.LIMIT_OF_RECORDS:
                                pod.usage.pop(0)
                        
                            pod.usage.append({'cpu': tmp_cpu, 'memory': tmp_mem})

                        break

                if not skip and pod_.metadata.namespace != 'default':
                    # this is new pod, add it to
                    pod = Pod(pod_.metadata, pod_.spec, pod_.status)
                    pod.is_alive = True
                    print('ADDED pod ' + pod.metadata.name)
                    self.all_pods.items.append(pod)

            if pod_.status.phase == 'Succeeded':
                for pod in self.all_pods.items:
                    if pod_.metadata.name == pod.metadata.name:
                        pod.status = pod_.status
                        index_pod = self.all_pods.getIndexPod(lambda x: x.metadata.name == pod.metadata.name)
                        self.all_pods.items[index_pod] = pod
                        this_pod = self.all_pods.getPod(lambda x: x.metadata.name == pod.metadata.name)
                        if this_pod.event is not None:
                            if this_pod.event == "service":
                                flag_incomplete = False
                                serv = self.all_services.getService(lambda x: x.id_ == this_pod.service_id)
                                #TODO Implemente bypass to avoid -> AttributeError: 'NoneType' object has no attribute 'vnfunctions'
                                if serv is not None:
                                    vnf = serv.vnfunctions.getVNF(lambda x: x.id_ == this_pod.id)
                                    vnf.completion_time = datetime.now()
                                    if vnf.completion_time > vnf.deadline:
                                        self.deadline_violations += 1
                                    if vnf.starting_time is None:
                                        vnf.starting_time = vnf.completion_time - timedelta(seconds=60)
                                        vnf.execution_time = abs((vnf.completion_time - vnf.starting_time).seconds)
                                    else:
                                        vnf.execution_time = abs((vnf.completion_time - vnf.starting_time).seconds)
                                    if serv.arrival_time is None:
                                        serv.arrival_time = vnf.starting_time - timedelta(seconds=30)
                                        vnf.flow_time = abs((vnf.completion_time - serv.arrival_time).seconds)
                                        vnf.waiting_time = abs((vnf.starting_time - serv.arrival_time).seconds)
                                    else:
                                        vnf.flow_time = abs((vnf.completion_time - serv.arrival_time).seconds)
                                    if vnf.id_ == serv.vnfunctions.items[-1].id_:
                                        serv.makespan = abs((vnf.completion_time - serv.arrival_time).seconds)
                                    if vnf.in_node is None:
                                        vnf.in_node = pod_.spec.node_name
                                        flag_incomplete = True
                                    vnf_index = serv.vnfunctions.getIndexVNF(lambda x: x.id_ == vnf.id_)
                                    serv.vnfunctions.items[vnf_index] = vnf
                                    if flag_incomplete:
                                        self.scheduled_VNFs += 1
                                        all_VNF_scheduled = serv.vnfunctions.areAllVNFScheduled(lambda x: x.in_node != None)
                                        if all_VNF_scheduled:
                                            self.scheduled_services += 1
                                    index_serv = self.all_services.getIndexService(lambda x: x.id_ == serv.id_)
                                    self.all_services.items[index_serv] = serv
                                    self.update_events_file(this_pod.event, vnf.name, vnf.id_, serv.id_, serv.name, vnf.service_arrival_time, None, vnf.r_rate, vnf.running_time, vnf.deadline, vnf.priority, vnf.waiting_time, vnf.starting_time, vnf.completion_time, vnf.execution_time, vnf.flow_time, vnf.in_node)
                                    if int(serv.running_time) > 0:
                                        self.delete_job(vnf.name)
                                    else:
                                        self.delete_deployment(vnf.name)
                                    self.all_pods.items.remove(this_pod)
                                    if self.scheduled_running_pods.isPodList(lambda x: x.metadata.name == i.metadata.name):
                                        self.scheduled_running_pods.items.remove(this_pod)
                                    print('Pod %s deleted' % this_pod.metadata.name)

                                else:
                                    tmp_owner = None
                                    for owner in this_pod.metadata.owner_references:
                                        tmp_owner = owner
                                        break
                                    print(tmp_owner)

                                    data = {}
                                    data = tmp_owner.to_dict()

                                    if data['kind'] == "ReplicaSet":
                                        self.delete_deployment(this_pod.metadata.labels['app'])
                                    
                                    elif data['kind'] == "Job":
                                        self.delete_job(this_pod.metadata.labels['app'])
                                        
                                    else:
                                        print('Error!!! Pod %s cannot be deleted' % this_pod.metadata.name)
                                    break

                            elif this_pod.event == "task":
                                task = self.all_tasks.getTask(lambda x: x.id_ == this_pod.id)
                                task.completion_time = datetime.now()
                                if task.completion_time > task.deadline:
                                    self.deadline_violations += 1
                                if task.starting_time is None:
                                    task.starting_time = task.completion_time - timedelta(seconds=60)
                                    task.execution_time = abs((task.completion_time - task.starting_time).seconds)
                                else:
                                    task.execution_time = abs((task.completion_time - task.starting_time).seconds)
                                if task.task_arrival_time is None:
                                    task.task_arrival_time = task.starting_time - timedelta(seconds=30)
                                    task.flow_time = abs((task.completion_time - task.task_arrival_time).seconds)
                                else:
                                    task.flow_time = abs((task.completion_time - task.task_arrival_time).seconds)
                                task_index = self.all_tasks.getIndexTask(lambda x: x.id_ == task.id_)
                                self.all_tasks.items[task_index] = task
                                self.update_events_file(this_pod.event, task.name, task.id_, None, None, None, task.task_arrival_time, task.r_rate, task.running_time, task.deadline, task.priority, task.waiting_time, task.starting_time, task.completion_time, task.execution_time, task.flow_time, task.in_node)
                                self.delete_job(task.name)
                                self.all_pods.items.remove(this_pod)
                                if self.scheduled_running_pods.isPodList(lambda x: x.metadata.name == i.metadata.name):
                                    self.scheduled_running_pods.items.remove(this_pod)
                                print('Pod %s deleted' % this_pod.metadata.name)

                            else:
                                print('Error!! Some event must be detected')
                            break

                        else:
                            tmp_owner = None
                            for owner in this_pod.metadata.owner_references:
                                tmp_owner = owner
                                break
                            print(tmp_owner)

                            data = {}
                            data = tmp_owner.to_dict()

                            if data['kind'] == "ReplicaSet":
                                self.delete_deployment(this_pod.metadata.labels['app'])
                            
                            elif data['kind'] == "Job":
                                self.delete_job(this_pod.metadata.labels['app'])
                                
                            else:
                                print('Error!!! Pod %s cannot be deleted' % this_pod.metadata.name)
                            break
            
            condition = None
            pod_dict = pod_.status.to_dict()
            if 'conditions' in pod_dict:
                if pod_.status.conditions is not None:
                    for c in pod_.status.conditions:
                        c_s = c.to_dict()
                        if c_s['type'] == 'PodScheduled':
                            condition = c_s
                    if pod_.status.phase == 'Pending' and condition['reason'] == 'Unschedulable':
                        for pod in self.all_pods.items:
                            if pod_.metadata.name == pod.metadata.name:
                                pod.status = pod_.status
                                index_pod = self.all_pods.getIndexPod(lambda x: x.metadata.name == pod.metadata.name)
                                self.all_pods.items[index_pod] = pod
                                this_pod = self.all_pods.getPod(lambda x: x.metadata.name == pod.metadata.name)
                                if this_pod.event is not None:
                                    if this_pod.event == "service":
                                        if self.all_services.isServiceList(lambda x: x.id_ == this_pod.service_id):
                                            serv = self.all_services.getService(lambda x: x.id_ == this_pod.service_id)
                                            numberVNFs = len(serv.vnfunctions.items)
                                            #self.all_services.items.remove(serv)
                                            for i in self.all_pods.items:
                                                if i.service_id == serv.id_:
                                                    self.all_pods.items.remove(i)
                                                    if self.scheduled_running_pods.isPodList(lambda x: x.metadata.name == i.metadata.name):
                                                        self.scheduled_running_pods.items.remove(i)
                                                    print('Pod %s deleted' % i.metadata.name)
                                                    
                                            vnf_serv_scheduled = 0
                                            for i in serv.vnfunctions.items:
                                                if i.in_node is not None:
                                                    vnf_serv_scheduled += 1

                                            self.rejected_services += 1
                                            self.rejected_VNFs += numberVNFs - vnf_serv_scheduled
                                            self.failed_vnfs += 1

                                            for i in serv.vnfunctions.items:
                                                if int(serv.running_time) > 0:
                                                    self.delete_job(i.name)
                                                else:
                                                    self.delete_deployment(i.name)
                                    
                                    elif this_pod.event == "task":
                                        task = self.all_tasks.getTask(lambda x: x.id_ == this_pod.id)

                                        self.all_tasks.items.remove(task)
                                        self.all_pods.items.remove(this_pod)
                                        if self.scheduled_running_pods.isPodList(lambda x: x.metadata.name == this_pod.metadata.name):
                                            self.scheduled_running_pods.items.remove(this_pod)
                                        print('Pod %s deleted' % pod.metadata.name)
                                        self.delete_job(task.name)
                                        self.rejected_tasks += 1
                                        self.failed_tasks += 1

                                    else:
                                        print('Error! Some event must be detected')
                                    break

                                else:
                                    tmp_owner = None
                                    for owner in this_pod.metadata.owner_references:
                                        tmp_owner = owner
                                        break
                                    print(tmp_owner)

                                    data = {}

                                    data = tmp_owner.to_dict()

                                    if data['kind'] == "ReplicaSet":
                                        self.delete_deployment(this_pod.metadata.labels['app'])
                                    
                                    elif data['kind'] == "Job":
                                        self.delete_job(this_pod.metadata.labels['app'])
                                        
                                    else:
                                        print('Error!!! Pod %s cannot be deleted' % this_pod.metadata.name)
                                    break

            if pod_.status.phase == 'Failed' or pod_.status.phase == 'Unknown' or pod_.status.phase == 'Error':
                for pod in self.all_pods.items:
                    if pod_.metadata.name == pod.metadata.name:
                        pod.status = pod_.status
                        index_pod = self.all_pods.getIndexPod(lambda x: x.metadata.name == pod.metadata.name)
                        self.all_pods.items[index_pod] = pod
                        this_pod = self.all_pods.getPod(lambda x: x.metadata.name == pod.metadata.name)
                        if this_pod.event is not None:
                            if this_pod.event == "service":
                                if self.all_services.isServiceList(lambda x: x.id_ == this_pod.service_id):
                                    serv = self.all_services.getService(lambda x: x.id_ == this_pod.service_id)
                                    numberVNFs = len(serv.vnfunctions.items)
                                    
                                    #self.all_services.items.remove(serv)
                                    for i in self.all_pods.items:
                                        if i.service_id == serv.id_:
                                            self.all_pods.items.remove(i)
                                            if self.scheduled_running_pods.isPodList(lambda x: x.metadata.name == i.metadata.name):
                                                self.scheduled_running_pods.items.remove(i)
                                            print('Pod %s deleted' % i.metadata.name)

                                    vnf_serv_scheduled = 0
                                    for i in serv.vnfunctions.items:
                                        if i.in_node is not None:
                                            vnf_serv_scheduled += 1

                                    self.rejected_services += 1
                                    self.rejected_VNFs += numberVNFs - vnf_serv_scheduled
                                    self.failed_vnfs += 1

                                    for i in serv.vnfunctions.items:
                                        if int(serv.running_time) > 0:
                                            self.delete_job(i.name)
                                        else:
                                            self.delete_deployment(i.name)
                            
                            elif this_pod.event == "task":
                                task = self.all_tasks.getTask(lambda x: x.id_ == this_pod.id)

                                self.all_tasks.items.remove(task)
                                self.all_pods.items.remove(this_pod)
                                if self.scheduled_running_pods.isPodList(lambda x: x.metadata.name == this_pod.metadata.name):
                                    self.scheduled_running_pods.items.remove(this_pod)
                                print('Pod %s deleted' % pod.metadata.name)
                                self.delete_job(task.name)
                                self.rejected_tasks += 1
                                self.failed_tasks += 1

                            else:
                                print('Error! Some event must be detected')
                            break

                        else:
                            tmp_owner = None
                            for owner in this_pod.metadata.owner_references:
                                tmp_owner = owner
                                break
                            print(tmp_owner)

                            if tmp_owner['kind'] == "ReplicaSet":
                                self.delete_deployment(this_pod.metadata.labels['app'])
                            
                            elif tmp_owner['kind'] == "Job":
                                self.delete_job(this_pod.metadata.labels['app'])
                                
                            else:
                                print('Error!!! Pod %s cannot be deleted' % this_pod.metadata.name)
                            break
                        
        self.status_lock.release()
        self.update_nodes()
        self.update_usage_file()

    def garbage_old_pods(self):
        """
        Remove dead pods from self.all_pods if Pod
        do not appeared in API response,
        dead Pods have self.is_alive set to False
        :return:
        """
        self.status_lock.acquire(blocking=True)
        for pod in self.all_pods.items[:]:
            if not pod.is_alive:
                self.all_pods.items.remove(pod)
        self.status_lock.release()

    def delete_deployment(self, deployment_name):
        '''
        Delete deployment object in Kubernetes framework
        '''
        try:
            api_response = self.apps_v1.delete_namespaced_deployment(name=deployment_name, namespace="default", body=client.V1DeleteOptions(propagation_policy='Foreground', grace_period_seconds=5))
            print(deployment_name + " deleted. status='%s'" % str(api_response.status))
        except ApiException as e:
            print("Exception when calling AppsV1Api->delete_namespaced_deployment: %s\n" % e)

    def delete_job(self, job_name):
        '''
        Delete job object in Kubernetes framework
        '''
        try:
            api_response = self.batch_v1.delete_namespaced_job(name=job_name, namespace="default", body=client.V1DeleteOptions(propagation_policy='Foreground', grace_period_seconds=5))
            print(job_name + " deleted. status='%s'" % str(api_response.status))
        except ApiException as e:
            print("Exception when calling BatchV1Api->delete_namespaced_job: %s\n" % e)

    def update_events_file(self, event, name, id, serv_id, serv_name, serv_arrival_time, task_arrival_time, demanded_rate, running_time, deadline, priority, waiting_time, starting_time, completion_time, execution_time, flow_time, assigned_node):
        '''
        Saves the events information to Events.csv file
        '''
        with open('Events.csv', 'a', buffering=1) as csvfile3:
            writer3 = csv.DictWriter(csvfile3, fieldnames=self.fieldnames_events)
            writer3.writerow({'Timestamp': str(datetime.now()),
                              'Event': str(event),
                              'Name': str(name),
                              'Id': str(id),
                              'Service_Id': str(serv_id),
                              'Service_Name': str(serv_name),
                              'Service_arrival_time': str(serv_arrival_time),
                              'Task_arrival_time': str(task_arrival_time),
                              'Demanded_rate': str(demanded_rate),
                              'Running_time': str(running_time),
                              'Deadline': str(deadline),
                              'Priority': str(priority),
                              'Waiting_time': str(waiting_time),
                              'Starting_time': str(starting_time),
                              'Completion_time': str(completion_time),
                              'Execution_time': str(execution_time),
                              'Flow_time': str(flow_time),
                              'Assigned_node': str(assigned_node)})

    def update_usage_file(self):
        '''
        Saves the usage node information to Usage.csv file and update the training dataset used for the regression model
        '''
        for node in self.all_nodes.items:

            with open('Usage.csv', 'a', buffering=1) as csvfile1:
                writer1 = csv.DictWriter(csvfile1, fieldnames=self.fieldnames_usage_node)
                if str(node.metadata.name).endswith('control-plane'):
                    net_in_pkt = self.net_usage_master()
                    node.net_in_pkt = net_in_pkt
                    date = str(datetime.now())
                    writer1.writerow({'Timestamp': date,
                                    'Node': str(node.metadata.name),
                                    'CPU': str(node.usage['cpu']),
                                    'Memory': str(node.usage['memory']), 
                                    'Proc_Capacity': str(node.proc_capacity),
                                    'Net_In_Pkt': str(net_in_pkt),
                                    'SoC': str(node.SoC)})
                    if (settings.SCHEDULING_CRITERIA == 0 or settings.SCHEDULING_CRITERIA == 1):
                        self.dataset_train = self.dataset_train.append({'Timestamp': date,
                                                                        'Node': node.metadata.name,
                                                                        'CPU': node.usage['cpu'],
                                                                        'Net_In_Pkt': net_in_pkt,
                                                                        'SoC_real': node.SoC,
                                                                        'SoC_pred': '0',
                                                                        'SoC_pred_upd': '0'}, ignore_index=True)
                else:
                    date = str(datetime.now())
                    writer1.writerow({'Timestamp': date,
                                    'Node': str(node.metadata.name),
                                    'CPU': str(node.usage['cpu']),
                                    'Memory': str(node.usage['memory']), 
                                    'Proc_Capacity': str(node.proc_capacity),
                                    'Net_In_Pkt': str(0.0),
                                    'SoC': str(node.SoC)})
                    if (settings.SCHEDULING_CRITERIA == 0 or settings.SCHEDULING_CRITERIA == 1):
                        self.dataset_train = self.dataset_train.append({'Timestamp': date,
                                                                        'Node': node.metadata.name,
                                                                        'CPU': node.usage['cpu'],
                                                                        'Net_In_Pkt': 0.0,
                                                                        'SoC_real': node.SoC,
                                                                        'SoC_pred': '0',
                                                                        'SoC_pred_upd': '0'}, ignore_index=True)

    def restart_dataset_train(self):
        '''
        Reset the training dataset by initializing it again
        '''
        self.status_lock.acquire(blocking=True)
        self.dataset_train = pd.DataFrame(columns=self.fieldnames_predictions)
        self.status_lock.release()
        print('Dataset to train was initialized')

    def net_usage_master(self, inf = "eth0"):   #change the inf variable according to the interface
        '''
        Get the number of packets going through a determined interface
        '''
        net_stat = psutil.net_io_counters(pernic=True, nowrap=True)[inf]
        net_in = net_stat.bytes_recv

        return round(net_in/1024/1024, 3)
        
