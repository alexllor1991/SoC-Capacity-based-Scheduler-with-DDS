import time
import os
import logging
from time import sleep
from datetime import datetime
from threading import Thread, Lock

from kubernetes import client, config
from kubernetes.client.rest import ApiException
from kubernetes.client.models.v1_container_image import V1ContainerImage

import settings
from node import Node, NodeList
from pod import Pod, PodList
from service import ServiceList, TaskList, Service, VNFunction, Task

NUMBER_OF_RETRIES = 7

logging.basicConfig(filename=settings.LOG_FILE, level=logging.INFO, datefmt='%Y-%m-%d %H:%M:%S',
                    format='%(asctime)s:%(levelname)s:%(message)s')

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

        configuration = client.Configuration()
        self.v1 = client.CoreV1Api(client.ApiClient(configuration))
        self.batch_v1 = client.BatchV1Api(client.ApiClient(configuration))
        self.apps_v1 = client.AppsV1Api(client.ApiClient(configuration))

        self.all_pods = PodList()
        self.all_nodes = NodeList()
        self.all_services = ServiceList()
        self.all_tasks = TaskList()
        self.pods_not_to_garbage = []

    def print_nodes_stats(self):
        """
        Print node stats
        :return:
        """
        for node in self.all_nodes.items:
            print(node.metadata.name, node.usage)

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
        
        print('Updating nodes')
        try:    
            if len(self.v1.list_node().items) != len(self.all_nodes.items):
                for node_ in self.v1.list_node().items:
                    node = Node(node_.metadata, node_.spec, node_.status)
                    node.update_node(self.all_pods)
                    self.all_nodes.items.append(node)
            for node_ in self.all_nodes.items:
                node_.update_node(self.all_pods)
                index = self.all_nodes.getIndexNode(lambda x: x.metadata.name == node_.metadata.name)
                self.all_nodes.items[index] = node_
            self.status_lock.release()
        except ApiException as e:
            print("Error: in list_node", e)

    def monitor_runner(self):
        """
        Run Pod monitor
        :return:
        """
        print('Monitor runner started')
        while True:
            #print('monitor tick')
            self.update_pods()
            time.sleep(self.time_interval)

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
                print('Waiting for pod %s' % new_pod.metadata.name)
                val = new_pod.fetch_usage()

                # do not add anything
                new_pod.usage = []

                if val == 0:
                    print('Pod %s ready...' % new_pod.metadata.name)
                    print(new_pod.usage)
                    break
                else:
                    print('Pod %s not ready...' % new_pod.metadata.name)

                    retries_not_ready += 1

                    if retries_not_ready == NUMBER_OF_RETRIES:
                        break

            else:
                print('Pod %s not found' % new_pod.metadata.name)

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
                        # found in collection, so update its usage
                        skip = True  # skip creating new Pod
                        pod.is_alive = True

                        res = pod.fetch_usage()

                        if res != 0:
                            if res == 404:
                                print('Metrics for pod %s not found ' % pod.metadata.name)
                            else:
                                print('Unknown metrics server error %s' % res)
                            break

                        # print('Updated metrics for pod %s' % pod.metadata.name)

                        break

                if not skip:
                    # this is new pod, add it to
                    pod = Pod(pod_.metadata, pod_.spec, pod_.status)
                    pod.is_alive = True
                    print('Added pod ' + pod.metadata.name)
                    self.all_pods.items.append(pod)

            if pod_.status.phase == 'Succeeded':
                for pod in self.all_pods.items:
                    if pod_.metadata.name == pod.metadata.name:
                        this_pod = self.all_pods.getPod(lambda x: x.metadata.name == pod.metadata.name)
                        if this_pod.event == "service":
                            serv = self.all_services.getService(lambda x: x.id_ == this_pod.service_id)
                            vnf = serv.vnfunctions.getVNF(lambda x: x.id_ == this_pod.id)
                            vnf.completion_time = datetime.now()
                            vnf.makespan = abs((vnf.completion_time - vnf.starting_time).seconds)
                            if vnf.id_ == serv.vnfunctions.items[-1].id_:
                                serv.makespan = abs((vnf.completion_time - serv.arrival_time).seconds)
                            vnf_index = serv.vnfunctions.getIndexVNF(lambda x: x.id_ == vnf.id_)
                            serv.vnfunctions.items[vnf_index] = vnf
                            index_serv = self.all_services.getIndexService(lambda x: x.id_ == serv.id_)
                            self.all_services.items[index_serv] = serv
                            vnf.to_dir()
                            serv.to_dir()
                            self.delete_deployment(vnf.name)
                            self.all_pods.items.remove(this_pod)
                            print('Pod %s deleted' % this_pod.metadata.name)
                        elif this_pod.event == "task":
                            task = self.all_tasks.getTask(lambda x: x.id_ == this_pod.id)
                            task.completion_time = datetime.now()
                            task.execution_time = abs((task.completion_time - task.starting_time).seconds)
                            task.flow_time = abs((task.completion_time - task.task_arrival_time).seconds)
                            task_index = self.all_tasks.getIndexTask(lambda x: x.id_ == task.id_)
                            self.all_tasks.items[task_index] = task
                            task.to_dir()
                            self.delete_job(task.name)
                            self.all_pods.items.remove(this_pod)
                            print('Pod %s deleted' % this_pod.metadata.name)
                        else:
                            print('Error!! Some event must be detected')

        print('Number of Pods ', len(self.all_pods.items))
        self.status_lock.release()
        #self.garbage_old_pods()

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
                print('Pod %s deleted' % pod.metadata.name)
        self.status_lock.release()

    def monitor_nodes(self):
        """
        Monitor Nodes usage
        :return:
        """
        pass

    def delete_deployment(self, deployment_name):
        try:
            api_response = self.apps_v1.delete_namespaced_deployment(name=deployment_name, namespace="default", body=client.V1DeleteOptions(propagation_policy='Foreground', grace_period_seconds=5))
            print("Deployment deleted. status='%s'" % str(api_response.status))
        except ApiException as e:
            print("Exception when calling AppsV1Api->delete_namespaced_deployment: %s\n" % e)

    def delete_job(self, job_name):
        try:
            api_response = self.batch_v1.delete_namespaced_job(name=job_name, namespace="default", body=client.V1DeleteOptions(propagation_policy='Foreground', grace_period_seconds=5))
            print("Job deleted. status='%s'" % str(api_response.status))
        except ApiException as e:
            print("Exception when calling BatchV1Api->delete_namespaced_job: %s\n" % e)
        
