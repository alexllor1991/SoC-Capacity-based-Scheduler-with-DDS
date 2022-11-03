#import rti.connextdds as dds_package
import sched
import time
import multiprocessing
import logging
import settings
import rticonnextdds_connector as rti
from datetime import datetime, timedelta
import threading
from pod import DataType, Pod

logging.basicConfig(filename=settings.LOG_FILE, level=logging.INFO, datefmt='%Y-%m-%d %H:%M:%S',
                    format='%(asctime)s:%(levelname)s:%(message)s')

class DDS_Algo:
    """
    Create a DDS entity to exchange network information
    with other entities in a DDS domain
    :param DDS Id:               Id to authenticate in DDS domain during discovery process
    :param DDS Participant Name: Participant name in DDS domain
    :param DDS Participant IP:   Participant ip to communicate with other participants  
    :param Main GC:              GC that manages the region where the cluster is deployed
    :return:
    """
    cluster = ""
    def __init__(self, dds_id, dds_participant_name, main_gc):
        self.own_key_name = "K8S_Master"
        self.main_gc = main_gc
        self.isActiveMainGC = True

        self.id = dds_id
        DDS_Algo.cluster = dds_participant_name

        self.sample_count = 0

        self.connector = rti.Connector("MyParticipantLibrary::kubernetes-control-plane1", "topologia.xml")

        print(self.connector.get_version)

        self.lock = threading.RLock()

    def read_samples(self, monitor, sche):
        from scheduler import Scheduler

        print("Waiting for publications...")
        self.lock.acquire()
        reader = self.connector.get_input("kubernetes-control-plane1-sub::kubernetes-control-plane1-dr")
        self.lock.release()
        reader.wait_for_publications()

        matched_pubs = reader.matched_publications
        for pub_info in matched_pubs:
            print(pub_info)

        print("Waiting for data...")
        while True:
            self.connector.wait()
            self.lock.acquire()
            reader.take()
            reader_release_flag = True
            writer_release_flag = False

            for sample in reader.samples.valid_data_iter:         
                data = sample.get_dictionary()
                identificador = data["Identificador"]
                sender = data["DestinationNode"]
                
                if identificador == DDS_Algo.cluster and sender != DDS_Algo.cluster:
                    node = data["NodeId"]
                    vnfName = data["TerminationPointId"]
                    cpuRequested = data["SourceNode"]
                    deploymentType = data["SourceNodeTp"]
                    serviceName = data["DestinationNodeTp"]
                    print(data)
                    if monitor.multideployed_pods.isPodList(lambda x: x.metadata.name == vnfName):
                        print("Placing VNF: " + vnfName)
                        sche.bind_to_node(pod_name=vnfName, node_name=node)
                        pod = monitor.multideployed_pods.getPod(lambda x: x.metadata.name == vnfName)
                        monitor.multideployed_pods.items.remove(pod)
                        is_scheduled = sche.is_pod_deployed(pod=pod)
                        service = monitor.all_services.getService(lambda x: x.id_ == pod.service_id)
                        if reader_release_flag:
                            self.lock.release()
                            reader_release_flag = False
                        if is_scheduled:
                            self.lock.acquire()
                            writer = self.connector.get_output("kubernetes-control-plane1-pub::kubernetes-control-plane1-dw")
                            writer.instance["Identificador"] = "VNF_Deployed"
                            writer.instance["NodeId"] = vnfName
                            writer.instance["TerminationPointId"] = service.name
                            writer.instance["DestinationNode"] = DDS_Algo.cluster
                            dt = int(datetime.now().timestamp() * 1000000000)
                            writer.write(source_timestamp=dt)
                            self.lock.release()
                            writer_release_flag = True
                        else:
                            self.lock.acquire()
                            writer = self.connector.get_output("kubernetes-control-plane1-pub::kubernetes-control-plane1-dw")
                            writer.instance["Identificador"] = "VNF_Rejected"
                            writer.instance["NodeId"] = vnfName
                            #writer.instance["TerminationPointId"] = service.name
                            writer.instance["DestinationNode"] = DDS_Algo.cluster
                            dt = int(datetime.now().timestamp() * 1000000000)
                            writer.write(source_timestamp=dt)
                            self.lock.release()
                            writer_release_flag = True
                    else:
                        print("Placing VNF: " + vnfName)
                        if deploymentType == "Deployment":
                            sche.create_deployment(event_name=vnfName, node_name=node, cpu_requested=cpuRequested)
                        else: 
                            sche.create_job(event_name=vnfName, node_name=node, cpu_requested=cpuRequested)
                        found = False
                        while not found:
                            for pod_ in monitor.v1.list_pod_for_all_namespaces().items:
                                if pod_.metadata.name.startswith(vnfName):
                                    new_pod = Pod(pod_.metadata, pod_.spec, pod_.status)
                                    monitor.all_pods.items.append(new_pod)
                                    print('New pod ADDED', new_pod.metadata.name)
                                    found = True
                                    break

                        pod = monitor.all_pods.getPod(lambda x: x.metadata.name.startswith(vnfName))
                        is_scheduled = sche.is_pod_deployed(pod=pod)
                        if reader_release_flag:
                            self.lock.release()
                            reader_release_flag = False
                        if is_scheduled:
                            self.lock.acquire()
                            writer = self.connector.get_output("kubernetes-control-plane1-pub::kubernetes-control-plane1-dw")
                            writer.instance["Identificador"] = "VNF_Deployed"
                            writer.instance["NodeId"] = vnfName
                            writer.instance["TerminationPointId"] = serviceName
                            writer.instance["DestinationNode"] = DDS_Algo.cluster
                            dt = int(datetime.now().timestamp() * 1000000000)
                            writer.write(source_timestamp=dt)
                            self.lock.release()
                            writer_release_flag = True
                        else:
                            self.lock.acquire()
                            writer = self.connector.get_output("kubernetes-control-plane1-pub::kubernetes-control-plane1-dw")
                            writer.instance["Identificador"] = "VNF_Rejected"
                            writer.instance["NodeId"] = vnfName
                            #writer.instance["TerminationPointId"] = service.name
                            writer.instance["DestinationNode"] = DDS_Algo.cluster
                            dt = int(datetime.now().timestamp() * 1000000000)
                            writer.write(source_timestamp=dt)
                            self.lock.release()
                            writer_release_flag = True

            if not writer_release_flag:
                self.lock.release() 