import logging
import settings
from kubernetes import client
from pod import PodList
import random

logging.basicConfig(filename=settings.LOG_FILE, level=logging.INFO, datefmt='%Y-%m-%d %H:%M:%S',
                    format='%(asctime)s:%(levelname)s:%(message)s')

class NodeList(object):
    def __init__(self):
        self.items = []

    def isNodeList(self, node_name):
        for i in self.items:
            if i.metadata.name == node_name:
                return True
            else:
                return False

    def getIndexNode(self, filter):
        for i,x in enumerate(self.items):
            if filter(x):
                return i
            else:
                print('Node not found')

class Node(object):
    def __init__(self, metadata_, spec_, status_):
        """
        :param V1ObjectMeta metadata_:
        :param V1NodeSpec spec_:
        :param V1NodeStatus status_:
        :return:
        """
        if type(metadata_) is not client.models.v1_object_meta.V1ObjectMeta:
            raise str("Passed invalid type")
        if type(spec_) is not client.models.V1NodeSpec:
            raise str("Passed invalid type")
        if type(status_) is not client.models.V1NodeStatus:
            raise str("Passed invalid type")

        self.usage = {}
        self.pods = PodList()

        self.metadata = metadata_
        self.spec = spec_
        self.status = status_
        self.proc_capacity = 0

    def update_node(self, pod_list):
        """
        Update Node Pods and usage attributes
        :param PodList pod_list: list of Pods
        :return:
        """
        self.pods = self.get_pods_on_node(pod_list)
        self.usage = self.get_node_usage()
        self.proc_capacity = self.process_capacity()

    def get_node_usage(self):
        """
        Calculate Node usage based on usage of
        Pods running on this node
        :return:
        """
        memory = 0
        cpu = 0
        for pod in self.pods.items:
            if pod.is_alive:
                # there can be pods not collected by garbage collector yet
                memory += int(pod.get_usage()['memory'])
                cpu += int(pod.get_usage()['cpu'])

        return {'cpu': cpu, 'memory': memory}

    def get_pods_on_node(self, pod_list):
        """
        Browse all available Pods in cluster and
        assign them to Node
        :param PodList pod_list: list of Pods
        :return PodList: return list of Pods running
            on this Node
        """
        result = PodList()
        for pod in pod_list.items:
            if pod.spec.node_name == self.metadata.name:
                result.items.append(pod)       

        return result

    def process_capacity(self):
        service_rate = random.randint(settings.MIN_PROCESS_CAPACITY, settings.MAX_PROCESS_CAPACITY)
        return service_rate