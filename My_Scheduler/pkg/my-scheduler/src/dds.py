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
        #self.ip_local_public_per_controller = {} # dictionary with key(participant_name) and values(list with ips)
        #self.discovered_GCs = multiprocessing.Manager().dict()  # concurrent dictionary with key(GC_ip) and values (subscriptionData)
        #self.failure_participants = multiprocessing.Manager().dict()  # concurrent dictionary with key(participantData_key) and values (participantData)
        #self.keys = multiprocessing.Manager().dict()  # concurrent dictionary with key(participant_ip) and values(particpantData_key)
        self.own_key_name = "K8S_Master"
        #self.alertWithinMs = 5
        self.main_gc = main_gc
        self.isActiveMainGC = True

        self.id = dds_id
        DDS_Algo.cluster = dds_participant_name

        #self.participant = None
        #self.topic = None
        #self.publisher = None
        #self.subscriber = None
        #self.topologia_datawriter = None
        #self.topologia_datareader = None

        self.sample_count = 0

        self.connector = rti.Connector("MyParticipantLibrary::kubernetes-control-plane1", "topologia.xml")

        print(self.connector.get_version)

        self.lock = threading.RLock()

        #self.writer = self.connector.get_output("kubernetes-control-plane1-pub::kubernetes-control-plane1-dw")
        #self.reader = self.connector.get_input("kubernetes-control-plane1-sub::kubernetes-control-plane1-dr")

        # try:
        #     factoryQos = dds_package.DomainParticipantFactoryQos()
        #     factoryQos.entity_factory.autoenable_created_entities = False

        #     pQos = dds_package.DomainParticipantQos()
        #     pQos.discovery_config.participant_liveliness_lease_duration.sec = 10
        #     pQos.discovery_config.participant_liveliness_lease_duration.nanosec = 0
        #     pQos.discovery_config.participant_liveliness_assert_period.sec = 2
        #     pQos.discovery_config.participant_liveliness_assert_period.nanosec = 0
        #     pQos.discovery_config.max_liveliness_loss_detection_period.sec = 1
        #     pQos.discovery_config.max_liveliness_loss_detection_period.nanosec = 0
        #     pQos.resource_limits.participant_user_data_max_length = 1024
        #     pQos.receiver_pool.buffer_size = 65530
        #     pQos.resource_limits.type_code_max_serialized_length = 65530
        #     pQos.resource_limits.type_object_max_serialized_length = 65530
        #     pQos.discovery.initial_peers.clear()
        #     pQos.discovery.initial_peers.append("239.255.0.1")
        #     pQos.discovery.initial_peers.append("6@builtin.udpv4://127.0.0.1")
        #     pQos.discovery.initial_peers.append("6@builtin.udpv4://147.83.113.39")
        #     pQos.discovery.initial_peers.append("6@builtin.udpv4://172.26.172.117")
        #     pQos.discovery.initial_peers.append("6@builtin.shmem://")
        #     pQos.discovery.multicast_receive_addresses.clear()
        #     pQos.discovery.multicast_receive_addresses.append("239.255.0.1")
        #     pQos.participant_name.name = dds_participant_name
        #     pQos.user_data.value = bytearray(self.id.encode())
            
        #     self.participant = dds_package.DomainParticipant(domain_id=0, qos=pQos, listener=None, mask=dds_package.StatusMask.NONE)

        #     if self.participant == None:
        #         print("Error while creating participant")
        #         return 

        #     # Participant properties give access to the builtin readers
        #     self.participant.participant_reader.bind_listener(BuiltinParticipantListener(), dds_package.StatusMask.DATA_AVAILABLE)
        #     self.participant.subscription_reader.bind_listener(BuiltinSubscriptionListener(), dds_package.StatusMask.DATA_AVAILABLE)
        #     self.participant.enable()

        # except Exception as e:
        #     print(str(e))

        # self.msg_type = dds_package.QosProvider("/app/topologia.xml").type("topologia")
        # #self.msg_type = dds_package.QosProvider("/home/alexllor/Phd_Degree/UC3M_Paper/K8S/my-scheduler/src/topologia.xml").type("topologia")
        # self.topic = dds_package.DynamicData.Topic(self.participant, "topologia", self.msg_type)
        
        # if self.topic == None:
        #     print("Unable to create topic\n")
        #     return

        # # Get default PublisherQos and add a partition
        # pubQos = dds_package.PublisherQos()
        # pubQos.partition.name.clear()
        # pubQos.partition.name.append("mainupc")
        # pubQos.partition.name.append("global")

        # # Publisher for communication between K8S_Master and GCs
        # self.publisher = dds_package.Publisher(self.participant, pubQos, None, dds_package.StatusMask.NONE)
        # if self.publisher == None:
        #     print("Unable to create publisher\n")
        #     return

        # # Get default DataWriterQos and add other parameters
        # dwqos = dds_package.DataWriterQos()
        # dwqos.liveliness.lease_duration.sec = 2
        # dwqos.liveliness.lease_duration.nanosec = 0
        # dwqos.reliability.kind = dds_package.ReliabilityKind.RELIABLE
        # dwqos.history.kind = dds_package.HistoryKind.KEEP_ALL
        # dwqos.durability.kind = dds_package.DurabilityKind.TRANSIENT_LOCAL
        # dwqos.reliability.max_blocking_time.sec = 2
        # dwqos.reliability.max_blocking_time.nanosec = 0
        # dwqos.resource_limits.max_samples = dds_package.LENGTH_UNLIMITED
        # dwqos.data_writer_protocol.rtps_reliable_writer.min_send_window_size = 20
        # dwqos.data_writer_protocol.rtps_reliable_writer.max_send_window_size = dwqos.data_writer_protocol.rtps_reliable_writer.min_send_window_size
        # dwqos.data_writer_protocol.rtps_reliable_writer.heartbeats_per_max_samples = dwqos.data_writer_protocol.rtps_reliable_writer.min_send_window_size
        # dwqos.data_writer_protocol.rtps_reliable_writer.min_nack_response_delay.sec = 0
        # dwqos.data_writer_protocol.rtps_reliable_writer.min_nack_response_delay.nanosec = 0
        # dwqos.data_writer_protocol.rtps_reliable_writer.max_nack_response_delay.sec = 0
        # dwqos.data_writer_protocol.rtps_reliable_writer.max_nack_response_delay.nanosec = 0
        # dwqos.data_writer_protocol.rtps_reliable_writer.fast_heartbeat_period.sec = 0
        # dwqos.data_writer_protocol.rtps_reliable_writer.fast_heartbeat_period.nanosec = self.alertWithinMs * 1000000
        # dwqos.data_writer_protocol.rtps_reliable_writer.max_heartbeat_retries = 7
        # dwqos.data_writer_protocol.rtps_reliable_writer.late_joiner_heartbeat_period.sec = 0
        # dwqos.data_writer_protocol.rtps_reliable_writer.late_joiner_heartbeat_period.nanosec = self.alertWithinMs * 1000000
        # dwqos.entity_name.name = dds_participant_name

        # # DataWriter for communication between K8S_Master and GCs
        # self.topologia_datawriter = dds_package.DynamicData.DataWriter(self.publisher, self.topic, dwqos, None, dds_package.StatusMask.NONE)
        # if self.topologia_datawriter == None:
        #     print("Unable to create data writer\n")
        #     return

        # #instancelocal_g = self.topologia_datawriter.lookup_instance()
        # #self.participant.ignore_datawriter(instancelocal_g)

        # # Get default SubscriberQos and add a partition
        # subQos = dds_package.SubscriberQos()
        # subQos.partition.name.clear()
        # subQos.partition.name.append("mainupc")
        # subQos.partition.name.append("global")

        # # Subscriber for communication between K8S_Master and GCs
        # self.subscriber = dds_package.Subscriber(self.participant, subQos, None, dds_package.StatusMask.NONE)
        # if self.subscriber == None:
        #     print("Unable to create subscriber\n")
        #     return

        # # Get default DataReaderQos and add other parameters
        # drqos = dds_package.DataReaderQos()
        # drqos.liveliness.lease_duration.sec = 2
        # drqos.liveliness.lease_duration.nanosec = 0
        # drqos.reliability.kind = dds_package.ReliabilityKind.RELIABLE
        # drqos.history.kind = dds_package.HistoryKind.KEEP_ALL
        # drqos.durability.kind = dds_package.DurabilityKind.TRANSIENT_LOCAL
        # drqos.reliability.max_blocking_time.sec = 2
        # drqos.reliability.max_blocking_time.nanosec = 0
        # drqos.resource_limits.max_samples = dds_package.LENGTH_UNLIMITED
        # drqos.data_reader_protocol.rtps_reliable_reader.min_heartbeat_response_delay.sec = 0
        # drqos.data_reader_protocol.rtps_reliable_reader.min_heartbeat_response_delay.nanosec = 0
        # drqos.data_reader_protocol.rtps_reliable_reader.max_heartbeat_response_delay.sec = 0
        # drqos.data_reader_protocol.rtps_reliable_reader.max_heartbeat_response_delay.nanosec = 0
        # drqos.entity_name.name = dds_participant_name

        # # DataReader for communication between K8S_Master and GCs
        # self.topologia_datareader = dds_package.DynamicData.DataReader(self.subscriber, self.topic, drqos, None, dds_package.StatusMask.NONE)
        # if self.topologia_datareader == None:
        #     print("Unable to create data reader\n")
        #     return
        # self.topologia_datareader.bind_listener(MsgListener(), dds_package.StatusMask.DATA_AVAILABLE)

        # # Get StatusCondition associated with the reader and set the mask to get data updates
        # status_condition_global = dds.StatusCondition(self.topologia_datareader)
        # if status_condition_global == None:
        #     print("Unable to get reader's condition\n")
        #     return

        # status_condition_global.enabled_statuses = dds.StatusMask.DATA_AVAILABLE

        # waitset = dds.WaitSet()
        # waitset.attach_condition(status_condition_global)

        # count = 0
        # while (self.sample_count == 0) or (count < self.sample_count):
        #     wait_timeout = dds.Duration(dds.Duration.infinite)
        #     active_condition = waitset.wait(wait_timeout)

        #     if status_condition_global in active_condition:
        #         self.on_data_available_global()

        #     time.sleep(1.0)

    def read_samples(self, monitor, sche):
        from scheduler import Scheduler

        print("Waiting for publications...")
        self.lock.acquire()
        reader = self.connector.get_input("kubernetes-control-plane1-sub::kubernetes-control-plane1-dr")
        self.lock.release()
        reader.wait_for_publications()

        #monitor.print_nodes_stats()
        #print(sche.scheduler_name)

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
                        #monitor.wait_for_multideployment_pod(new_pod = pod)
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
                                #print(pod_.metadata.name)
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

# A Listenener class for DDS Participant data
# class BuiltinParticipantListener(dds_package.ParticipantBuiltinTopicData.NoOpDataReaderListener):
#     def __init__(self):
#         super(BuiltinParticipantListener, self).__init__()

#     def on_data_available(self, reader):
#         # only process previously unseen Participants
#         builtin_reader = dds_package.ParticipantBuiltinTopicData.DataReader(reader=reader)

#         with builtin_reader.select().state(dds_package.DataState.new_instance).take() as samples:
#             for sample in filter(lambda s: s.info.valid, samples):

#                 if sample.data.user_data.value.__sizeof__() > 0:
#                     user_data = sample.data.user_data.value
#                     participant_data = "".join((chr(c) for c in user_data))
#                     if (participant_data == "ac") or (participant_data == "k8s_master"):
#                          participant_remote = builtin_reader.subscriber.participant
#                          participant_remote.ignore_participant(sample.info.instance_handle)

        # with reader.select().state(dds.DataState.new_instance).take() as samples:
        #     for sample in filter(lambda s: s.info.valid, samples):
        #         # Convert Participant user data to a string
        #         user_data = sample.data.user_data.value
        #         user_auth = "".join((chr(c) for c in user_data))
        #         key = sample.data.key

        #         print("Built-in Reader: found participant")
        #         print("\tkey->'{:08X} {:08X} {:08X}'".format(*key.value[:3]))
        #         print("\tuser_data->'{}'".format(user_auth))
        #         print("\tinstance_handle: {}".format(sample.info.instance_handle))

        #         # Check if the password match.Otherwise, ignore the participant.
        #         if user_auth != self.expected_password:
        #             print("Bad authorization, ignoring participant")
        #             participant = reader.subscriber.participant
        #             participant.ignore_participant(sample.info.instance_handle)

# Create a Subscription listener, print discovered DataReader information
# class BuiltinSubscriptionListener(dds_package.SubscriptionBuiltinTopicData.NoOpDataReaderListener, DDS_Algo):
#     def __init__(self):
#         super(BuiltinSubscriptionListener, self).__init__()

#     def on_data_available(self, reader):
#         # only process previously unseen DataReaders
#         builtin_reader = dds_package.SubscriptionBuiltinTopicData.DataReader(reader=reader)
#         while True:
#             sample = builtin_reader.take_next()
#             subscriptionData = sample.data
#             info = sample.info

#             if info.state == dds_package.InstanceState.ALIVE:
#                 ip = info.source_guid.fget()
#                 ip_address = self.byteToInt(ip[0]) + "." + self.byteToInt(ip[1]) + "." + self.byteToInt(ip[2]) + "." + self.byteToInt(ip[3])

#                 print("DataReader (New)" + 
#                     " messageNum: " + str(info.reception_sequence_number.fget()) +
#                     " name: \"" + subscriptionData.subscription_name.name + "\"" +
#                     " topic: " + subscriptionData.topic_name +
#                     " type: " + subscriptionData.type_name +
#                     " created at: " + info.source_timestamp + 
#                     " detected at: " + info.reception_timestamp +
#                     " ip_address: " + ip_address + 
#                     " full details: " + str(subscriptionData))

#                 if not ip_address in self.discovered_GCs.keys():
#                     self.discovered_GCs[ip_address] = subscriptionData

#                 if (subscriptionData.subscription_name.name == self.main_gc) and not self.isActiveMainGC:
#                     self.isActiveMainGC = True

#                     if self.publisher.qos.partition.name[0] == "backup":
#                         self.publisher.qos.partition.name.insert(0, "mainupc")
#                         print("Setting partition to: " + self.publisher.qos.partition.name[0])
                        
#                     if self.subscriber.qos.partition.name[0] == "backup":
#                         self.subscriber.qos.partition.name.insert(0, "mainupc")
#                         print("Setting partition to: " + self.subscriber.qos.partition.name[0])

#                 if not self.isActiveMainGC:
#                     self.publisher.qos.partition.name.insert(0, "backup")
#                     print("Setting partition to: " + self.publisher.qos.partition.name[0])
#                     self.subscriber.qos.partition.name.insert(0, "backup")
#                     print("Setting partition to: " + self.subscriber.qos.partition.name[0])

#             else:
#                 dissapearReason = ""
#                 if info.state == dds_package.InstanceState.NOT_ALIVE_DISPOSED:
#                     dissapearReason = "delected"
#                 else:
#                     dissapearReason = "lost connection"

#                 if info.valid:
#                     ip = info.source_guid.fget()
#                     ip_address = self.byteToInt(ip[0]) + "." + self.byteToInt(ip[1]) + "." + self.byteToInt(ip[2]) + "." + self.byteToInt(ip[3])

#                     print("DataReader (Dissapeared - " + dissapearReason + "): " + 
#                         " messageNum: " + str(info.reception_sequence_number.fget()) +
#                         " name: \"" + subscriptionData.subscription_name.name + "\"" +
#                         " created at: " + info.source_timestamp + 
#                         " detected at: " + info.reception_timestamp +
#                         " ip_address: " + ip_address + 
#                         " full details: " + str(subscriptionData))
                    
#                 else:
#                     ip = info.source_guid.fget()
#                     ip_address = self.byteToInt(ip[0]) + "." + self.byteToInt(ip[1]) + "." + self.byteToInt(ip[2]) + "." + self.byteToInt(ip[3])

#                     gc_fail = self.discovered_GCs.get(ip_address)

#                     print("DataReader (Dissapeared - " + dissapearReason + "): " + 
#                         " messageNum: " + str(info.reception_sequence_number.fget()) +
#                         " name: \"" + gc_fail.subscription_name.name + "\"" +
#                         " created at: " + info.source_timestamp + 
#                         " detected at: " + info.reception_timestamp +
#                         " ip_address: " + ip_address)                    

#                     if gc_fail.subscription_name.name == self.main_gc:
#                         print(gc_fail.subscription_name.name + " has failed. Executing failover")

#                         if self.isActiveMainGC:
#                             self.isActiveMainGC = False

#                         self.publisher.qos.partition.name.insert(0, "backup")
#                         print("Setting partition to: " + self.publisher.qos.partition.name[0])
#                         self.subscriber.qos.partition.name.insert(0, "backup")
#                         print("Setting partition to: " + self.subscriber.qos.partition.name[0])

        # with reader.select().state(dds.DataState.new_instance).take() as samples:
        #     for sample in filter(lambda s: s.info.valid, samples):
        #         participant_key = sample.data.participant_key
        #         key = sample.data.key

        #         print("Built-in Reader: found subscriber")
        #         print(
        #             "\tparticipant_key->'{:08X} {:08X} {:08X}'".format(
        #                 *participant_key.value[0:3]
        #             )
        #         )
        #         print("\tkey->'{:08X} {:08X} {:08X}'".format(*key.value[0:3]))
        #         print("instance_handle: {}".format(sample.info.instance_handle))

# A listener for msg samples
# class MsgListener(dds_package.DynamicData.NoOpDataReaderListener, DDS_Algo):

#     def on_data_available(self, reader):
#         from scheduler import Scheduler
#         #data_reader = dds_package.DynamicData.DataReader(reader=reader)

#         #while True:
#             #try:
#                 #sample = data_reader.take_next()
#                 #data = sample.data
#                 #info = sample.info
#         with reader.take() as samples:
#             for sample in filter(lambda s: s.info.valid, samples):
#                 data = sample.data
#                 #if info.valid:
#                 # Deploy VNF in indicated node 
#                 if data["Identificador"] == DDS_Algo.cluster:
#                     node = data["NodeId"]
#                     vnfName = data["TerminationPointId"]
#                     if Scheduler.monitor.multideployed_pods.isPodList(lambda x: x.metadata.name == vnfName):
#                         Scheduler.bind_to_node(vnfName, node)
#                         pod = Scheduler.monitor.multideployed_pods.getPod(lambda x: x.metadata.name == vnfName)
#                         Scheduler.monitor.multideployed_pods.items.remove(pod)
#                         is_scheduled = Scheduler.is_pod_deployed(pod)
#                         service = Scheduler.monitor.all_services.getService(lambda x: x.id_ == pod.service_id)
#                         if is_scheduled:
#                             vnf_topic = dds_package.DynamicData(self.msg_type)
#                             vnf_topic["Identificador"] = "VNF_Deployed"
#                             vnf_topic["NodeId"] = vnfName
#                             vnf_topic["TerminationPointId"] = service.name
#                             self.topologia_datawriter.write(vnf_topic, dds_package.InstanceHandle())

            #except Exception as e:
            #    print(str(e))

