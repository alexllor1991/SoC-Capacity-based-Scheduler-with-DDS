import os
from time import sleep, time
from datetime import datetime, timedelta
from dateutil import parser
from threading import Thread
import multiprocessing 
import logging
import settings
import itertools
from monitor import ClusterMonitor
from node import NodeList
from pod import DataType, Pod
from service import ServiceList, Service, VNFunction, Task

from kubernetes import client, config, watch
from kubernetes.client.rest import ApiException

logging.basicConfig(filename=settings.LOG_FILE, level=logging.INFO, datefmt='%Y-%m-%d %H:%M:%S',
                    format='%(asctime)s:%(levelname)s:%(message)s')

class Scheduler:
    def __init__(self):
        self.monitor = ClusterMonitor()
        self.watcherpod = watch.Watch()
        configuration = client.Configuration()
        self.v1 = client.CoreV1Api(client.ApiClient(configuration))
        self.all_services = ServiceList()
        self.priority_list = multiprocessing.Manager().list()

        self.vnf = VNFunction(None,None,None,None)

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

        p2 = Thread(target=self.scheduler_runner)
        p2.start()

        self.init_log_file()

        while True:
            try:
                for eventpod in self.watcherpod.stream(self.v1.list_pod_for_all_namespaces):    

                    if eventpod['type'] == 'ADDED' and eventpod['object'].spec.scheduler_name == self.scheduler_name:
                        if not self.all_services.isServiceList(eventpod['object'].metadata.annotations['service_id']):
                             new_service = Service(eventpod['object'].metadata.annotations['service_id'], eventpod['object'].metadata.annotations['service_name'], eventpod['object'].metadata.annotations['service_deadline'], eventpod['object'].metadata.annotations['service_priority'], eventpod['object'].metadata.annotations['service_running_time'])
                             print ('New service ADDED', new_service.name)
                             new_service.arrival_time = datetime.now()
                             new_service.to_dir()
                             self.all_services.items.append(new_service)
                             print (self.all_services.items)
                        serv = self.all_services.getService(lambda x: x.id_ == eventpod['object'].metadata.annotations['service_id'])
                        
                        if not serv.vnfunctions.isVNFList_ID(eventpod['object'].metadata.annotations['id_vnf']):
                            new_vnf = VNFunction(eventpod['object'].metadata.annotations['id_vnf'], eventpod['object'].metadata.name, eventpod['object'].metadata.annotations['required_service_rate'], eventpod['object'].metadata.annotations['service_id'])
                            new_vnf.deadline = parser.parse(serv.deadline)
                            new_vnf.priority = serv.priority
                            new_vnf.running_time = serv.running_time
                            new_vnf.service_arrival_time = serv.arrival_time
                            serv.vnfunctions.items.append(new_vnf)
                            serv.vnfunctions.items.sort(key=lambda x: x.id_, reverse=False)

                        self.vnf = serv.vnfunctions.getVNF(lambda x: x.id_ == eventpod['object'].metadata.annotations['id_vnf'])
                        
                        print(serv.vnfunctions.items)
                        index = self.all_services.getIndexService(lambda x: x.id_ == serv.id_)
                        self.all_services.items[index] = serv
                        
                        new_pod = Pod(eventpod['object'].metadata, eventpod['object'].spec, eventpod['object'].status)
                        new_pod.demanded_processing = self.vnf.r_rate
                        new_pod.id = self.vnf.id_
                        new_pod.service_id = self.vnf.serviceid
                        new_pod.deadline = self.vnf.deadline
                        new_pod.running_time_task = self.vnf.running_time
                        new_pod.service_arrival_time = self.vnf.service_arrival_time
                        new_pod.priority = self.vnf.priority
                        new_pod.scheduling_criteria = settings.SCHEDULING_CRITERIA
                        print('New pod ADDED', new_pod.metadata.name)
                        new_pod.to_dir()
                        
                        print('Putting new pod in priority list')
                        if settings.PRIORITY_LIST_CRITERIA == 0: # priority list is sorted by taking into account the service deadline
                            print('Sorting by deadline')
                            prov_list = list(self.priority_list)
                            prov_list.append(new_pod)
                            prov_list.sort(key=lambda x: x.deadline, reverse=False)
                            self.priority_list = prov_list.copy()
                        elif settings.PRIORITY_LIST_CRITERIA == 1: # priority list is sorted by taking into account the service priority
                            print('Sorting by priority')
                            prov_list = list(self.priority_list)
                            prov_list.append(new_pod)
                            prov_list.sort(key=lambda x: x.priority, reverse=True)
                            self.priority_list = prov_list.copy()
                        elif settings.PRIORITY_LIST_CRITERIA == 2: # priority list is sorted dynamically by taking into account an equation
                            print('Sorting dynamically')
                            task_delay = abs((new_pod.deadline - datetime.now() - timedelta(seconds=int(new_pod.running_time_task))).seconds) 
                            waiting_time = abs((datetime.now() - new_pod.service_arrival_time).seconds)
                            rank = (settings.delta_1 * task_delay) - (settings.delta_2 * waiting_time)
                            new_pod.rank = rank
                            prov_list = list(self.priority_list)
                            prov_list.append(new_pod)
                            new_pod.to_dir()
                            prov_list.sort(key=lambda x: x.rank, reverse=False)
                            self.priority_list = prov_list.copy()
                        else:
                            print('None priority list criteria has been set')

                        print('pod introduced in priority list')

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

    def scheduler_runner (self):
        while True:
            if len(self.priority_list) > 0:
                try:
                    print('Printing priority list')
                    print(self.priority_list)

                    self.monitor.update_nodes()

                    for node in self.monitor.all_nodes.items:
                        logging.info(node.metadata.name + ' ' + str(node.usage['memory']) + ' ' + str(node.usage['cpu']) + ' ' + str(node.proc_capacity))
            
                    self.monitor.print_nodes_stats()

                    obj = self.priority_list.pop(0)

                    new_node = self.choose_node(obj)

                    if new_node is not None:
                        self.bind_to_node(obj.metadata.name, new_node.metadata.name)
                        """
                        without this cluster for 2nd and next Pods in deployment looks the same,
                        so all Pods from deployment are placed on the same Node, we want to avoid this
                        block scheduling thread until newly created Pod is ready
                        """
                        self.monitor.wait_for_pod(obj)
                        serv = self.all_services.getService(lambda x: x.id_ == obj.service_id)
                        vnf = serv.vnfunctions.getVNF(lambda x: x.id_ == obj.id)
                        vnf.starting_time = datetime.now()
                        vnf.waiting_time = abs((vnf.starting_time - obj.service_arrival_time).seconds)
                        vnf.in_node = new_node.metadata.name
                        if vnf.id_==serv.vnfunctions.items[0].id_:
                            serv.waiting_time_first_VNF = vnf.waiting_time
                        vnf_index = serv.vnfunctions.getIndexVNF(lambda x: x.id_ == vnf.id_)
                        serv.vnfunctions.items[vnf_index] = vnf
                        index_serv = self.all_services.getIndexService(lambda x: x.id_ == serv.id_)
                        self.all_services.items[index_serv] = serv
                        vnf.to_dir()
                        serv.to_dir()
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
        return_node_list = NodeList()
        
        if pod.spec.node_name is not None:
            for node in self.monitor.all_nodes.items:
                if pod.spec.node_name == node.metadata.name and node.spec.unschedulable is not True:
                    return_node_list.items.append(node)
        else:
            for node in self.monitor.all_nodes.items:
                if pod.scheduling_criteria == 0 and pod.parse_usage_data(pod.spec.containers[0].resources.requests['memory'], DataType.MEMORY) < (settings.TOTAL_MEMORY - node.usage['memory']) and pod.parse_usage_data(pod.spec.containers[0].resources.requests['cpu'], DataType.CPU) < (settings.TOTAL_CPU - node.usage['cpu']) and node.spec.unschedulable is not True:
                     return_node_list.items.append(node)
                elif pod.scheduling_criteria == 1 and pod.parse_usage_data(pod.spec.containers[0].resources.requests['memory'], DataType.MEMORY) < (settings.TOTAL_MEMORY - node.usage['memory']) and node.spec.unschedulable is not True:
                    return_node_list.items.append(node)
                elif pod.scheduling_criteria == 2 and pod.parse_usage_data(pod.spec.containers[0].resources.requests['cpu'], DataType.CPU) < (settings.TOTAL_CPU - node.usage['cpu']) and node.spec.unschedulable is not True:
                    return_node_list.items.append(node)
                elif pod.scheduling_criteria == 3 and int(pod.demanded_processing) < node.proc_capacity and node.spec.unschedulable is not True:
                    return_node_list.items.append(node)
                else: 
                    print('None node can meet the pod requirements')

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

            best_node = node_list.items[0]

            for node in node_list.items:
                print(node.metadata.name, node.usage)

                if pod.scheduling_criteria == 1:
                    print('Running scoring process taking into account available memory')
                    if (settings.TOTAL_MEMORY - node.usage['memory']) > (settings.TOTAL_MEMORY - best_node.usage['memory']):
                        best_node = node

                elif pod.scheduling_criteria == 2:
                    print('Running scoring process taking into account available cpu')
                    if (settings.TOTAL_CPU - node.usage['cpu']) > (settings.TOTAL_CPU - best_node.usage['cpu']):
                        best_node = node

                elif pod.scheduling_criteria == 3:
                    print('Running scoring process taking into account the best processing capacity')
                    if node.proc_capacity > best_node.proc_capacity:
                        best_node = node

                else:
                    print('None running scoring process was selected')
                    pass
                print('Current best: ' + best_node.metadata.name)

        print('Selected node:')
        print(best_node.metadata.name, best_node.usage)
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
