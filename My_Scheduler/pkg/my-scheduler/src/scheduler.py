import os
import csv
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
from pod import DataType, Pod, PodList
from service import ServiceList, TaskList, Service, VNFunction, Task

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
        self.priority_list = multiprocessing.Manager().list()

        self.vnf = VNFunction(None,None,None,None)
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
                                   'Deadline_violations']

        self.fieldnames_usage_node = ['Timestamp',
                                      'Node',
                                      'CPU',
                                      'Memory',
                                      'Proc_Capacity']

        # self.fieldnames_events = ['Timestamp',
        #                           'Event',
        #                           'Name',
        #                           'Id',
        #                           'Service_Id',
        #                           'Service_Name',
        #                           'Service_arrival_time',
        #                           'Task_arrival_time',
        #                           'Demanded_rate',
        #                           'Running_time',
        #                           'Deadline',
        #                           'Priority',
        #                           'Waiting_time',
        #                           'Starting_time',
        #                           'Completion_time',
        #                           'Execution_time',
        #                           'Flow_time',
        #                           'Assigned_node']

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

        while True:
            try:
                for eventpod in self.watcherpod.stream(self.v1.list_pod_for_all_namespaces):    

                    if eventpod['type'] == 'ADDED' and eventpod['object'].spec.scheduler_name == self.scheduler_name:
                        if eventpod['object'].metadata.annotations['event'] == "service":
                            if not self.monitor.all_services.isServiceList(lambda x: x.id_ == eventpod['object'].metadata.annotations['service_id']):
                                new_service = Service(eventpod['object'].metadata.annotations['service_id'], eventpod['object'].metadata.annotations['service_name'], eventpod['object'].metadata.annotations['service_deadline'], eventpod['object'].metadata.annotations['service_priority'], eventpod['object'].metadata.annotations['service_running_time'])
                                print ('New service ADDED', new_service.name)
                                new_service.arrival_time = datetime.now()
                                #new_service.to_dir()
                                self.monitor.all_services.items.append(new_service)
                                self.monitor.requested_services += 1
                                print(self.monitor.all_services.items)
                            serv = self.monitor.all_services.getService(lambda x: x.id_ == eventpod['object'].metadata.annotations['service_id'])
                        
                            if not serv.vnfunctions.isVNFList(lambda x: x.id_ == eventpod['object'].metadata.annotations['vnf_id']):
                                new_vnf = VNFunction(eventpod['object'].metadata.annotations['vnf_id'], eventpod['object'].metadata.annotations['vnf_name'], eventpod['object'].metadata.annotations['required_service_rate'], eventpod['object'].metadata.annotations['service_id'])
                                print ('New VNF ADDED', new_vnf.name)
                                new_vnf.deadline = parser.parse(serv.deadline)
                                new_vnf.priority = serv.priority
                                new_vnf.running_time = serv.running_time
                                new_vnf.service_arrival_time = serv.arrival_time
                                self.monitor.update_events_file(eventpod['object'].metadata.annotations['event'], new_vnf.name, new_vnf.id_, eventpod['object'].metadata.annotations['service_id'], eventpod['object'].metadata.annotations['service_name'], new_vnf.service_arrival_time, None, new_vnf.r_rate, new_vnf.running_time, new_vnf.deadline, new_vnf.priority, None, None, None, None, None, None)
                                serv.vnfunctions.items.append(new_vnf)
                                serv.vnfunctions.items.sort(key=lambda x: x.id_, reverse=False)
                                self.monitor.requested_VNFs += 1
                            self.vnf = serv.vnfunctions.getVNF(lambda x: x.id_ == eventpod['object'].metadata.annotations['vnf_id'])
                        
                            print(serv.vnfunctions.items)
                            index = self.monitor.all_services.getIndexService(lambda x: x.id_ == serv.id_)
                            self.monitor.all_services.items[index] = serv

                        elif eventpod['object'].metadata.annotations['event'] == "task":
                            if not self.monitor.all_tasks.isTaskList(lambda x: x.id_ == eventpod['object'].metadata.annotations['task_id']):
                                new_task_deadline = parser.parse(eventpod['object'].metadata.annotations['task_deadline'])
                                new_task = Task(eventpod['object'].metadata.annotations['task_id'], eventpod['object'].metadata.annotations['task_name'], new_task_deadline, eventpod['object'].metadata.annotations['task_priority'], eventpod['object'].metadata.annotations['task_running_time'], eventpod['object'].metadata.annotations['task_required_rate'])
                                print ('New task ADDED', new_task.name)
                                new_task.task_arrival_time = datetime.now()
                                #new_task.to_dir()
                                self.monitor.update_events_file(eventpod['object'].metadata.annotations['event'], new_task.name, new_task.id_, None, None, None, new_task.task_arrival_time, new_task.r_rate, new_task.running_time, new_task.deadline, new_task.priority, None, None, None, None, None, None)
                                self.monitor.all_tasks.items.append(new_task)
                                self.monitor.requested_tasks += 1
                                print(self.monitor.all_tasks.items)
                            self.task = self.monitor.all_tasks.getTask(lambda x: x.id_ == eventpod['object'].metadata.annotations['task_id'])

                        else:
                            print('None event was detected')

                        new_pod = Pod(eventpod['object'].metadata, eventpod['object'].spec, eventpod['object'].status)
                        new_pod.scheduling_criteria = settings.SCHEDULING_CRITERIA

                        if eventpod['object'].metadata.annotations['event'] == "service":
                            new_pod.demanded_processing = self.vnf.r_rate
                            new_pod.id = self.vnf.id_
                            new_pod.service_id = self.vnf.serviceid
                            new_pod.deadline = self.vnf.deadline
                            new_pod.running_time = self.vnf.running_time
                            new_pod.service_arrival_time = self.vnf.service_arrival_time
                            new_pod.priority = self.vnf.priority
                            new_pod.event = 'service'
                        elif eventpod['object'].metadata.annotations['event'] == "task":
                            new_pod.demanded_processing = self.task.r_rate
                            new_pod.id = self.task.id_
                            new_pod.deadline = self.task.deadline
                            new_pod.running_time = self.task.running_time
                            new_pod.task_arrival_time = self.task.task_arrival_time
                            new_pod.priority = self.task.priority
                            new_pod.event = 'task'
                        else:
                            print('None event was detected')
                        
                        print('New pod ADDED', new_pod.metadata.name)
                        new_pod.to_dir()
                        if not self.monitor.all_pods.isPodList(lambda x: x.metadata.name == new_pod.metadata.name):
                            self.monitor.all_pods.items.append(new_pod)
                        else:
                            index_pod = self.monitor.all_pods.getIndexPod(lambda x: x.metadata.name == new_pod.metadata.name)
                            self.monitor.all_pods.items[index_pod] = new_pod
                        
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
                            task_delay = abs((new_pod.deadline - datetime.now() - timedelta(seconds=int(new_pod.running_time))).seconds) 
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

                        print('Pod introduced in priority list')

            except Exception as e:
                print(str(e))

    def init_files(self):
        """
        Initialize log file, insert node names at
        the first line of a file
        :return:
        """
        self.monitor.update_nodes()

        # nodes_names = ''

        # for node in self.monitor.all_nodes.items:
        #     nodes_names += str(node.metadata.name)
        #     nodes_names += ':'

        # nodes_names = nodes_names[:-1]
        # logging.info(nodes_names)

        with open('Usage.csv', 'w', buffering=1) as csvfile1:
            writer1 = csv.DictWriter(csvfile1, fieldnames=self.fieldnames_usage_node)
            writer1.writeheader()

        with open('Results.csv', 'w', buffering=1) as csvfile2:
            writer2 = csv.DictWriter(csvfile2, fieldnames=self.fieldnames_results)
            writer2.writeheader()

        with open('Events.csv', 'w', buffering=1) as csvfile3:
            writer3 = csv.DictWriter(csvfile3, fieldnames=self.monitor.fieldnames_events)
            writer3.writeheader()

    def update_usage_result_files(self):

        for node in self.monitor.all_nodes.items:
            logging.info(node.metadata.name + ' ' + 'CPU:' + str(node.usage['cpu']) + ' ' + 'Memory:' + str(node.usage['memory']) + ' ' + 'Proc_Capacity:' + str(node.proc_capacity))

            with open('Usage.csv', 'a', buffering=1) as csvfile1:
                writer1 = csv.DictWriter(csvfile1, fieldnames=self.fieldnames_usage_node)
                writer1.writerow({'Timestamp': str(datetime.now()),
                                  'Node': str(node.metadata.name),
                                  'CPU': str(node.usage['cpu']),
                                  'Memory': str(node.usage['memory']), 
                                  'Proc_Capacity': str(node.proc_capacity)})

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
                             'Deadline_violations': str(self.monitor.deadline_violations)})

    # def update_events_file(self, event, name, id, serv_id, serv_name, serv_arrival_time, task_arrival_time, demanded_rate, running_time, deadline, priority, waiting_time, starting_time, completion_time, execution_time, flow_time, assigned_node):
        
    #     with open('Events.csv', 'a', buffering=1) as csvfile3:
    #         writer3 = csv.DictWriter(csvfile3, fieldnames=self.fieldnames_events)
    #         writer3.writerow({'Timestamp': str(datetime.now()),
    #                           'Event': str(event),
    #                           'Name': str(name),
    #                           'Id': str(id),
    #                           'Service_Id': str(serv_id),
    #                           'Service_Name': str(serv_name),
    #                           'Service_arrival_time': str(serv_arrival_time),
    #                           'Task_arrival_time': str(task_arrival_time),
    #                           'Demanded_rate': str(demanded_rate),
    #                           'Running_time': str(running_time),
    #                           'Deadline': str(deadline),
    #                           'Priority': str(priority),
    #                           'Waiting_time': str(waiting_time),
    #                           'Starting_time': str(starting_time),
    #                           'Completion_time': str(completion_time),
    #                           'Execution_time': str(execution_time),
    #                           'Flow_time': str(flow_time),
    #                           'Assigned_node': str(assigned_node)})

    def scheduler_runner(self):
        while True:
            if len(self.priority_list) > 0:
                try:
                    print('Printing priority list')
                    print(self.priority_list)

                    self.monitor.update_nodes()

                    self.update_usage_result_files()
            
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
                            index_serv = self.monitor.all_services.getIndexService(lambda x: x.id_ == serv.id_)
                            self.monitor.all_services.items[index_serv] = serv
                            self.monitor.update_nodes()
                            self.update_usage_result_files()
                            self.monitor.update_events_file(obj.event, vnf.name, vnf.id_, serv.id_, serv.name, vnf.service_arrival_time, None, vnf.r_rate, vnf.running_time, vnf.deadline, vnf.priority, vnf.waiting_time, vnf.starting_time, None, None, None, vnf.in_node)
                            #vnf.to_dir()
                            serv.to_dir()
                        elif obj.event == "task":
                            task = self.monitor.all_tasks.getTask(lambda x: x.id_ == obj.id)
                            task.starting_time = datetime.now()
                            task.waiting_time = abs((task.starting_time - obj.task_arrival_time).seconds)
                            task.in_node = new_node.metadata.name
                            self.monitor.scheduled_tasks += 1
                            task_index = self.monitor.all_tasks.getIndexTask(lambda x: x.id_ == task.id_)
                            self.monitor.all_tasks.items[task_index] = task
                            self.monitor.update_nodes()
                            self.update_usage_result_files()
                            self.monitor.update_events_file(obj.event, task.name, task.id_, None, None, None, task.task_arrival_time, task.r_rate, task.running_time, task.deadline, task.priority, task.waiting_time, task.starting_time, None, None, None, task.in_node)
                            #task.to_dir()
                        else:
                            print('Event could not be updated')
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
                if pod.scheduling_criteria == 0 and pod.parse_usage_data(pod.spec.containers[0].resources.requests['memory'], DataType.MEMORY) < (self.monitor.max_mem_per_node - node.usage['memory']) and pod.parse_usage_data(pod.spec.containers[0].resources.requests['cpu'], DataType.CPU) < (self.monitor.max_cpu_per_node - node.usage['cpu']) and node.spec.unschedulable is not True:
                     return_node_list.items.append(node)
                elif pod.scheduling_criteria == 1 and pod.parse_usage_data(pod.spec.containers[0].resources.requests['memory'], DataType.MEMORY) < (self.monitor.max_mem_per_node - node.usage['memory']) and node.spec.unschedulable is not True:
                    return_node_list.items.append(node)
                elif pod.scheduling_criteria == 2 and pod.parse_usage_data(pod.spec.containers[0].resources.requests['cpu'], DataType.CPU) < (self.monitor.max_cpu_per_node - node.usage['cpu']) and node.spec.unschedulable is not True:
                    return_node_list.items.append(node)
                elif pod.scheduling_criteria == 3 and int(pod.demanded_processing) < node.proc_capacity and node.spec.unschedulable is not True:
                    return_node_list.items.append(node)
                else: 
                    print('None node can meet the pod requirements')
                    if pod.event == "service":
                        serv = self.monitor.all_services.getService(lambda x: x.id_ == pod.service_id)
                        numberVNFs = len(serv.vnfunctions.items)
                        for i in serv.vnfunctions.items[:]:
                            self.monitor.delete_deployment(i.name)
                        self.monitor.all_services.items.remove(serv)
                        for i in self.monitor.all_pods.items[:]:
                            if i.service_id == serv.id_:
                                self.monitor.all_pods.items.remove(i)
                                print('Pod %s deleted' % i.metadata.name)
                        self.monitor.rejected_services += 1
                        self.monitor.rejected_VNFs += numberVNFs
                    elif pod.event == "task":
                        task = self.monitor.all_tasks.getTask(lambda x: x.id_ == pod.id)
                        self.monitor.delete_job(task.name)
                        self.monitor.all_tasks.items.remove(task)
                        self.monitor.all_pods.items.remove(pod)
                        print('Pod %s deleted' % pod.metadata.name)
                        self.monitor.rejected_tasks += 1
                    else:
                        print('Error!! Some event must be detected')

        return return_node_list

    #@staticmethod
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
                print(node.metadata.name, node.usage)

                if pod.scheduling_criteria == 1:
                    print('Running scoring process taking into account available memory')
                    if (self.monitor.max_mem_per_node - node.usage['memory']) > (self.monitor.max_mem_per_node - best_node.usage['memory']):
                        best_node = node

                elif pod.scheduling_criteria == 2:
                    print('Running scoring process taking into account available cpu')
                    if (self.monitor.max_cpu_per_node - node.usage['cpu']) > (self.monitor.max_cpu_per_node - best_node.usage['cpu']):
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
