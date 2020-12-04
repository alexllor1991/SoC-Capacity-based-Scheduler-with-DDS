import os
from time import sleep
from datetime import datetime as dt
from threading import Thread
import logging
import settings
import itertools
from monitor import ClusterMonitor
from node import NodeList
from pod import Pod, SchedulingCriteria
from service import Priority, ServicePriorityList, ServiceList, Service, VNFunction, Task

from kubernetes import client, config, watch

logging.basicConfig(filename=settings.LOG_FILE, level=logging.INFO, datefmt='%Y-%m-%d %H:%M:%S',
                    format='%(asctime)s:%(levelname)s:%(message)s')

class PriorityListCriteria(Enum):
    SERVICEDEADLINE = 0
    SERVICEPRIORITY = 1
    DYNAMIC = 2

class Scheduler:
    def __init__(self):
        self.monitor = ClusterMonitor()
        self.watcherpod = watch.Watch()
        self.watcherdep = watch.Watch()
        configuration = client.Configuration()
        self.v1 = client.CoreV1Api(client.ApiClient(configuration))
        self.v1ext = client.ExtensionsV1beta1Api(client.ApiClient(configuration))

        self.all_services = ServiceList()
        self.priority_list = ServicePriorityList()

        self.vnf = VNFunction()

        self.scheduler_name = 'my-scheduler'

    def run(self):
        """
        Main thread, run and listen for events,
        If an event occurred, call monitor.update_nodes()
        and proceed scoring and scheduling process.
        """
        print('Scheduler running')

        p1 = Thread(target=self.monitor.monitor_runner)
        p1.start()

        self.init_log_file()

        while True:
            try:
                for (eventpod, eventdep) in zip(self.watcherpod.stream(self.v1.list_pod_for_all_namespaces), self.watcherdep.stream(v1ext.list_deployment_for_all_namespaces)):
                    #print('Event type ', event['type'])

                    if eventdep['type'] == 'ADDED' and (eventdep['object'].kind == 'Deployment' or eventdep['object'].kind == 'ReplicaSet') and eventdep['object'].spec.template.spec.scheduler_name == self.scheduler_name:
                        print('Event object ', eventdep['object'].kind)

                        if not self.all_services.isServiceList(eventdep['object'].metadata.annotations.service_id): 
                            new_service = Service(eventdep['object'].metadata.annotations.service_id, eventdep['object'].metadata.annotations.service_name, eventdep['object'].metadata.annotations.service_deadline, eventdep['object'].metadata.annotations.service_priority, eventdep['object'].metadata.annotations.service_runningtime)
                            print ('New service ADDED', new_service.name)
                            new_service.arrival_time = dt.now().isoformat()
                            self.all_services.items.append(new_service)
                        serv = self.all_services.getService(lambda x: x.id_ == eventdep['object'].metadata.annotations.service_id)

                        new_vnf = VNFunction(eventdep['object'].metadata.annotations.id_vnf, eventdep['object'].metadata.name, eventdep['object'].metadata.annotations.required_service_rate, eventdep['object'].metadata.annotations.service_id)
                        self.vnf = new_vnf
                        if not serv.isVNFList_ID(new_vnf.id_):
                            serv.vnfunctions.items.append(new_vnf)
                        index = self.all_services.getIndexService(lambda x: x.id_ == serv.id_)
                        self.all_services.items[index] = serv

                    if eventpod['type'] == 'ADDED' and eventpod['object'].spec.scheduler_name == self.scheduler_name:
                        new_pod = Pod(eventpod['object'].metadata, eventpod['object'].spec, eventpod['object'].status)
                        new_pod.demanded_processing = self.vnf.r_rate
                        print('New pod ADDED', new_pod.metadata.name)

                        vnf = self.all_services.items

                        for i in self.priority_list.items:

                            self.monitor.update_nodes()

                            for node in self.monitor.all_nodes.items:
                                logging.info(node.metadata.name + ' ' + str(node.usage['memory']) + ' ' + str(node.usage['cpu']))

                            self.monitor.print_nodes_stats()

                            new_node = self.choose_node(i)

                            if new_node is not None:
                                self.bind_to_node(i.metadata.name, new_node.metadata.name)
                                """
                                without this cluster for 2nd and next Pods in deployment looks the same,
                                so all Pods from deployment are placed on the same Node, we want to avoid this
                                block scheduling thread until newly created Pod is ready
                                """
                                self.monitor.wait_for_pod(i)
                                self.monitor.update_pods()
                            else:
                                print('Pod cannot be scheduled..')
                                """
                                when Pod cannot be scheduled it is being deleted and after
                                couple seconds new Pod is being created and another attempt
                                of scheduling this Pod is being made
                                """
            except Exception as e:
                print(str(e))

    def init_log_file(self):
        """
        Initialize log file, insert node names at
        the first line of a file
        :return:
        """
        self.monitor.update_nodes()

        nodes_names = ''

        for node in self.monitor.all_nodes.items:
            nodes_names += str(node.metadata.name)
            nodes_names += ':'

        nodes_names = nodes_names[:-1]
        logging.info(nodes_names)

    def choose_node(self, pod):
        """
        Method that brings together all methods
        responsible for choosing best Node for a Pod
        :param pod.Pod pod: Pod to be scheduled
        :return node.Node: return best selected Node for Pod,
            None if Pod cannot be scheduled
        """
        possible_nodes = self.filter_nodes(pod)

        print('Possible nodes')
        for node in possible_nodes.items:
            print(node.metadata.name)

        selected_node = self.score_nodes(pod, possible_nodes)

        if selected_node is not None:
            print('Selected Node', selected_node.metadata.name)
        else:
            print('No node was being selected')
        return selected_node

    def filter_nodes(self, pod):
        """
        Filter Nodes in self.monitor.all_nodes
        which can run selected Pod
        :param pod.Pod pod: Pod to be scheduled
        :return node.NodeList: List of Node which
            satisfy Pod requirements
        """
        # TODO add SLA requirements of app related to CPU and Memory.
        return_node_list = NodeList()

        if pod.spec.node_name is not None:
            for node in self.monitor.all_nodes.items:
                if pod.spec.node_name == node.metadata.name and node.spec.unschedulable is not True:
                    return_node_list.items.append(node)
        else:
            #print('All nodes can be used for Pod %s ' % pod.metadata.name)
            for node in self.monitor.all_nodes.items:
                if pod.scheduling_criteria == SchedulingCriteria.MIXED and pod.spec.containers.requests['memory'] < (settings.TOTAL_MEMORY - node.usage['memory']) and pod.spec.containers.requests['cpu'] < (settings.TOTAL_CPU - node.usage['cpu']) and node.spec.unschedulable is not True:
                     return_node_list.items.append(node)
                elif pod.scheduling_criteria == SchedulingCriteria.MEMORY and pod.spec.containers.requests['memory'] < (settings.TOTAL_MEMORY - node.usage['memory']) and node.spec.unschedulable is not True:
                    return_node_list.items.append(node)
                elif pod.scheduling_criteria == SchedulingCriteria.CPU and pod.spec.containers.requests['cpu'] < (settings.TOTAL_CPU - node.usage['cpu']) and node.spec.unschedulable is not True:
                    return_node_list.items.append(node)
                elif pod.scheduling_criteria == SchedulingCriteria.FASTPROCESSING and pod.demanded_processing < node.proc_capacity and node.spec.unschedulable is not True:
                    return_node_list.items.append(node)
                if node.spec.unschedulable is not True:
                    # TODO check labels there and decide if Node can be used for pod
                    return_node_list.items.append(node)

        return return_node_list

    @staticmethod
    def score_nodes(pod, node_list):
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
            print('Running scoring process ')

            best_node = node_list.items[0]

            for node in node_list.items:
                print(node.metadata.name, node.usage)

                if pod.scheduling_criteria == SchedulingCriteria.MEMORY:
                    if node.usage['memory'] < best_node.usage['memory']:
                        best_node = node

                elif pod.scheduling_criteria == SchedulingCriteria.CPU:
                    if node.usage['cpu'] < best_node.usage['cpu']:
                        best_node = node

                elif pod.scheduling_criteria == SchedulingCriteria.FASTPROCESSING:
                    if node.proc_capacity < best_node.proc_capacity:
                        best_node = node

                elif pod.scheduling_criteria == SchedulingCriteria.CPU:
                    pass
                    # current_best =

        for node in node_list.items:
            print(node.metadata.name, node.usage)

        print('Selected node:')
        print(best_node.metadata.name, best_node.usage, best_node.proc_capacity)
        return best_node

    def calculate_score(self, pod):
        """
        Calculate score for a Node using defined formula
        :param pod.Pod pod:
        :return int: Node score
        """

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

    @staticmethod
    def pass_to_scheduler(name_, namespace_, scheduler_name_='default-scheduler'):
        """
        Pass deployment to be scheduled by different scheduler
        :param str scheduler_name_: name of new scheduler, which will
            schedule this deployment
        :param str name_: name of deployment
        :param str namespace_: namespace of deployment
        :return str: return http response code
        """
        url = '/apis/extensions/v1beta1/namespaces/' + namespace_ + '/deployments/' + name_
        headers = {'Accept': 'application/json', 'Content-Type': 'application/strategic-merge-patch+json'}
        body = {"spec": {"template": {"spec": {"schedulerName": scheduler_name_}}}}

        api_client = client.ApiClient()
        try:
            response = api_client.call_api(url, 'PATCH', header_params=headers, body=body)
        except Exception as e:
            return int(str(e)[1:4])

        return int(response[1])

def main():
    try:
        config.load_incluster_config()
    except:
        config.load_kube_config() 
    scheduler = Scheduler()
    scheduler.run()

if __name__ == '__main__':
    main()
