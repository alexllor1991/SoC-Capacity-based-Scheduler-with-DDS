#! /usr/bin/python3

import os
import yaml
import random
import time
import math
import statistics
import matplotlib.pyplot as plt
import string
import uuid
from datetime import datetime, timedelta

name_vnf = 'vnf-'
name_serv = 'serv-'
name_task = 'task-'
serv_id = ''
random_string_serv = ''
deadline_service = ''
running_service_time = ''
service_priority = ''
service_rate = ''
_lambda = 5  # Execution events rate events/min
_number_events = 25
_inter_event_times = []
_event_numbers = []
_event_times = []
_event_time = 0
deadline_difference = 2

def get_random_string(length):
    letters = string.ascii_lowercase
    result_str = ''.join(random.sample(letters, length))
    return result_str

print('EVENT_NUM,INTER_EVENT_T,EVENT_T')

for i in range(_number_events):
    _event_numbers.append(i)
    n = random.random()

    _inter_event_time = -math.log(1.0 - n) / _lambda   # inverse of CDF(Cumulative Probability Distribution) to get the actual time between consecutive events in the Poisson process
    _inter_event_times.append(_inter_event_time)

    _event_time = _event_time + _inter_event_time
    _event_times.append(_event_time)

    print(str(i) +',' + str(_inter_event_time) + ',' + str(_event_time))

    generate_event = random.randint(0, 2)  # 0 generate a task, 1 generate a persistent service and 2 generate a service during some time

    if generate_event == 0:
        #with open(r'c:\Users\aleja\Dropbox\Phd_Degree\Intership\Kubernetes_Fault_Tolerant\Scheduling_code\My_Scheduler\deployment\scenario_1\simple_task_template.yml') as fileread:
        #with open(r'/mnt/c/Users/aleja/Dropbox/Phd_Degree/Intership/Kubernetes_Fault_Tolerant/Scheduling_code/My_Scheduler/deployment/scenario_1/simple_task_template.yml') as fileread:
        with open(r'/mnt/c/Users/user/Dropbox/Phd_Degree/Intership/Kubernetes_Fault_Tolerant/Scheduling_code/My_Scheduler/deployment/scenario_1/simple_task_template.yml') as fileread:
            documents = yaml.full_load(fileread)

            random_string = get_random_string(7)
            random_cpu = random.randint(200, 250)
            random_memory = random.randint(200, 500)
            start = datetime.now()
            end = start + timedelta(minutes=deadline_difference)
            deadline_task = str(start + (end - start) * random.randint(1, 10))
            documents['metadata']['name'] = name_task + random_string
            documents['spec']['template']['metadata']['labels']['app'] = name_task + random_string
            documents['spec']['template']['spec']['containers'][0]['name'] = name_task + random_string
            documents['spec']['template']['metadata']['annotations']['task_id'] = str(uuid.uuid4())
            documents['spec']['template']['metadata']['annotations']['task_name'] = name_task + random_string
            documents['spec']['template']['metadata']['annotations']['task_deadline'] = str(deadline_task)
            documents['spec']['template']['metadata']['annotations']['task_running_time'] = str(random.randint(1, 100))
            documents['spec']['template']['metadata']['annotations']['task_priority'] = str(random.randint(0,10))
            documents['spec']['template']['metadata']['annotations']['task_required_rate'] = str(random.randint(1, 500))
            documents['spec']['template']['spec']['containers'][0]['resources']['requests']['memory'] = str(str(random_memory) + 'Mi')
            documents['spec']['template']['spec']['containers'][0]['resources']['requests']['cpu'] = str(str(random_cpu) + 'm')
            #documents['spec']['template']['spec']['containers'][0]['resources']['limits']['memory'] = str(str(random.randint(random_memory, 600)) + 'M')
            #documents['spec']['template']['spec']['containers'][0]['resources']['limits']['cpu'] = str(str(random.randint(random_cpu, 350)) + 'm')

            print(documents)

            #with open(r'c:\Users\aleja\Dropbox\Phd_Degree\Intership\Kubernetes_Fault_Tolerant\Scheduling_code\My_Scheduler\deployment\scenario_1\simple_task_generated.yml', 'w') as filewrite:
            #with open(r'/mnt/c/Users/aleja/Dropbox/Phd_Degree/Intership/Kubernetes_Fault_Tolerant/Scheduling_code/My_Scheduler/deployment/scenario_1/simple_task_generated.yml', 'w') as filewrite:
            with open(r'/mnt/c/Users/user/Dropbox/Phd_Degree/Intership/Kubernetes_Fault_Tolerant/Scheduling_code/My_Scheduler/deployment/scenario_1/simple_task_generated.yml', 'w') as filewrite:
                yaml.dump(documents, filewrite)

            os.system('sudo kubectl apply -f simple_task_generated.yml')
    
    if generate_event == 1:

        #with open(r'c:\Users\aleja\Dropbox\Phd_Degree\Intership\Kubernetes_Fault_Tolerant\Scheduling_code\My_Scheduler\deployment\scenario_1\persistent_service_template.yml') as fileread:
        #with open(r'/mnt/c/Users/aleja/Dropbox/Phd_Degree/Intership/Kubernetes_Fault_Tolerant/Scheduling_code/My_Scheduler/deployment/scenario_1/persistent_service_template.yml') as fileread:
        with open(r'/mnt/c/Users/user/Dropbox/Phd_Degree/Intership/Kubernetes_Fault_Tolerant/Scheduling_code/My_Scheduler/deployment/scenario_1/persistent_service_template.yml') as fileread:
            documents = yaml.full_load(fileread)

            amount_vnf_per_service = random.randint(1, 10)
            for i in range(1, amount_vnf_per_service + 1):
                if serv_id == '':
                    serv_id = str(uuid.uuid4())
                    random_string_serv = get_random_string(7)
                    start = datetime.now()
                    end = start + timedelta(minutes=deadline_difference)
                    deadline_service = str(start + (end - start) * random.randint(1, 10))
                    running_service_time = "0"
                    service_priority = str(random.randint(0,10))
                    service_rate = str(random.randint(1, 500))
                random_string = get_random_string(7)
                random_cpu = random.randint(100, 250)
                random_memory = random.randint(200, 500)
                documents['metadata']['name'] = name_vnf + random_string
                documents['spec']['selector']['matchLabels']['app'] = name_vnf + random_string
                documents['spec']['template']['metadata']['labels']['app'] = name_vnf + random_string
                documents['spec']['template']['spec']['containers'][0]['name'] = name_vnf + random_string
                documents['spec']['template']['metadata']['annotations']['service_id'] = serv_id
                documents['spec']['template']['metadata']['annotations']['service_name'] = name_serv + random_string_serv
                documents['spec']['template']['metadata']['annotations']['service_deadline'] = deadline_service
                documents['spec']['template']['metadata']['annotations']['service_running_time'] = running_service_time
                documents['spec']['template']['metadata']['annotations']['service_priority'] = service_priority
                documents['spec']['template']['metadata']['annotations']['required_service_rate'] = service_rate
                documents['spec']['template']['metadata']['annotations']['vnf_id'] = str(i)
                documents['spec']['template']['metadata']['annotations']['vnf_name'] = name_vnf + random_string
                documents['spec']['template']['spec']['containers'][0]['resources']['requests']['memory'] = str(str(random_memory) + 'Mi')
                documents['spec']['template']['spec']['containers'][0]['resources']['requests']['cpu'] = str(str(random_cpu) + 'm')
                #documents['spec']['template']['spec']['containers'][0]['resources']['limits']['memory'] = str(str(random.randint(random_memory, 600) + 50) + 'M')
                #documents['spec']['template']['spec']['containers'][0]['resources']['limits']['cpu'] = str(str(random.randint(random_cpu, 350) + 50) + 'm')
                print(documents)

                if i == 1:
                    #with open(r'c:\Users\aleja\Dropbox\Phd_Degree\Intership\Kubernetes_Fault_Tolerant\Scheduling_code\My_Scheduler\deployment\scenario_1\persistent_service_generated.yml', 'w') as filewrite:
                    #with open(r'/mnt/c/Users/aleja/Dropbox/Phd_Degree/Intership/Kubernetes_Fault_Tolerant/Scheduling_code/My_Scheduler/deployment/scenario_1/persistent_service_generated.yml', 'w') as filewrite:
                    with open(r'/mnt/c/Users/user/Dropbox/Phd_Degree/Intership/Kubernetes_Fault_Tolerant/Scheduling_code/My_Scheduler/deployment/scenario_1/persistent_service_generated.yml', 'w') as filewrite:
                        yaml.dump(documents, filewrite)
                else:
                    #with open(r'c:\Users\aleja\Dropbox\Phd_Degree\Intership\Kubernetes_Fault_Tolerant\Scheduling_code\My_Scheduler\deployment\scenario_1\persistent_service_generated.yml', 'a') as filewrite:
                    #with open(r'/mnt/c/Users/aleja/Dropbox/Phd_Degree/Intership/Kubernetes_Fault_Tolerant/Scheduling_code/My_Scheduler/deployment/scenario_1/persistent_service_generated.yml', 'a') as filewrite:
                    with open(r'/mnt/c/Users/user/Dropbox/Phd_Degree/Intership/Kubernetes_Fault_Tolerant/Scheduling_code/My_Scheduler/deployment/scenario_1/persistent_service_generated.yml', 'a') as filewrite:
                        filewrite.write('---')
                        filewrite.write("\n")
                        yaml.dump(documents, filewrite)

            os.system('sudo kubectl apply -f persistent_service_generated.yml')
            serv_id = ''

    if generate_event == 2:

        #with open(r'c:\Users\aleja\Dropbox\Phd_Degree\Intership\Kubernetes_Fault_Tolerant\Scheduling_code\My_Scheduler\deployment\scenario_1\limited_service_template.yml') as fileread:
        #with open(r'/mnt/c/Users/aleja/Dropbox/Phd_Degree/Intership/Kubernetes_Fault_Tolerant/Scheduling_code/My_Scheduler/deployment/scenario_1/limited_service_template.yml') as fileread:
        with open(r'/mnt/c/Users/user/Dropbox/Phd_Degree/Intership/Kubernetes_Fault_Tolerant/Scheduling_code/My_Scheduler/deployment/scenario_1/limited_service_template.yml') as fileread:
            documents = yaml.full_load(fileread)

            amount_vnf_per_service = random.randint(1, 10)
            for i in range(1, amount_vnf_per_service + 1):
                if serv_id == '':
                    serv_id = str(uuid.uuid4())
                    random_string_serv = get_random_string(7)
                    start = datetime.now()
                    end = start + timedelta(minutes=deadline_difference)
                    deadline_service = str(start + (end - start) * random.randint(1, 10))
                    running_service_time = str(random.randint(1, 100))
                    service_priority = str(random.randint(0,10))
                    service_rate = str(random.randint(0, 500))
                random_string = get_random_string(7)
                random_cpu = random.randint(100, 250)
                random_memory = random.randint(200, 500)
                documents['metadata']['name'] = name_vnf + random_string
                documents['spec']['template']['metadata']['labels']['app'] = name_vnf + random_string
                documents['spec']['template']['spec']['containers'][0]['name'] = name_vnf + random_string
                documents['spec']['template']['metadata']['annotations']['service_id'] = serv_id
                documents['spec']['template']['metadata']['annotations']['service_name'] = name_serv + random_string_serv
                documents['spec']['template']['metadata']['annotations']['service_deadline'] = deadline_service
                documents['spec']['template']['metadata']['annotations']['service_running_time'] = running_service_time
                documents['spec']['template']['metadata']['annotations']['service_priority'] = service_priority
                documents['spec']['template']['metadata']['annotations']['required_service_rate'] = service_rate
                documents['spec']['template']['metadata']['annotations']['vnf_id'] = str(i)
                documents['spec']['template']['metadata']['annotations']['vnf_name'] = name_vnf + random_string
                documents['spec']['template']['spec']['containers'][0]['resources']['requests']['memory'] = str(str(random_memory) + 'Mi')
                documents['spec']['template']['spec']['containers'][0]['resources']['requests']['cpu'] = str(str(random_cpu) + 'm')
                #documents['spec']['template']['spec']['containers'][0]['resources']['limits']['memory'] = str(str(random.randint(random_memory, 600) + 50) + 'M')
                #documents['spec']['template']['spec']['containers'][0]['resources']['limits']['cpu'] = str(str(random.randint(random_cpu, 350) + 50) + 'm')
                print(documents)

                if i == 1:
                    #with open(r'c:\Users\aleja\Dropbox\Phd_Degree\Intership\Kubernetes_Fault_Tolerant\Scheduling_code\My_Scheduler\deployment\scenario_1\limited_service_generated.yml', 'w') as filewrite:
                    #with open(r'/mnt/c/Users/aleja/Dropbox/Phd_Degree/Intership/Kubernetes_Fault_Tolerant/Scheduling_code/My_Scheduler/deployment/scenario_1/limited_service_generated.yml', 'w') as filewrite:
                    with open(r'/mnt/c/Users/user/Dropbox/Phd_Degree/Intership/Kubernetes_Fault_Tolerant/Scheduling_code/My_Scheduler/deployment/scenario_1/limited_service_generated.yml', 'w') as filewrite:
                        yaml.dump(documents, filewrite)
                else:
                    #with open(r'c:\Users\aleja\Dropbox\Phd_Degree\Intership\Kubernetes_Fault_Tolerant\Scheduling_code\My_Scheduler\deployment\scenario_1\limited_service_generated.yml', 'a') as filewrite:
                    #with open(r'/mnt/c/Users/aleja/Dropbox/Phd_Degree/Intership/Kubernetes_Fault_Tolerant/Scheduling_code/My_Scheduler/deployment/scenario_1/limited_service_generated.yml', 'a') as filewrite:
                    with open(r'/mnt/c/Users/user/Dropbox/Phd_Degree/Intership/Kubernetes_Fault_Tolerant/Scheduling_code/My_Scheduler/deployment/scenario_1/limited_service_generated.yml', 'a') as filewrite:
                        filewrite.write('---')
                        filewrite.write("\n")
                        yaml.dump(documents, filewrite)

            os.system('sudo kubectl apply -f limited_service_generated.yml')
            serv_id = ''

    time.sleep(60 * _inter_event_time)

fig = plt.figure()
fig.suptitle('Times between consecutive events in a simulated Poisson process')
plot, = plt.plot(_event_numbers, _inter_event_times, 'bo-', label='Inter-event time')
plt.legend(handles=[plot])
plt.xlabel('Event number')
plt.ylabel('Time')
#plt.savefig('/mnt/c/Users/aleja/Dropbox/Phd_Degree/Intership/Kubernetes_Fault_Tolerant/Scheduling_code/My_Scheduler/deployment/scenario_1/Inter_event_time.png')
plt.savefig('/mnt/c/Users/user/Dropbox/Phd_Degree/Intership/Kubernetes_Fault_Tolerant/Scheduling_code/My_Scheduler/deployment/scenario_1/Inter_event_time.png')

fig = plt.figure()
fig.suptitle('Absolute times of consecutive events in a simulated Poisson process')
plot, = plt.plot(_event_numbers, _event_times, 'bo-', label='Absolute time of event')
plt.legend(handles=[plot])
plt.xlabel('Event number')
plt.ylabel('Time')
#plt.savefig('/mnt/c/Users/aleja/Dropbox/Phd_Degree/Intership/Kubernetes_Fault_Tolerant/Scheduling_code/My_Scheduler/deployment/scenario_1/Absolute_time_event.png')
plt.savefig('/mnt/c/Users/user/Dropbox/Phd_Degree/Intership/Kubernetes_Fault_Tolerant/Scheduling_code/My_Scheduler/deployment/scenario_1/Absolute_time_event.png')

_interval_nums = []
_num_events_in_interval = []
_interval_num = 1
_num_events = 0

print('INTERVAL_NUM,NUM_EVENTS')

for i in range(len(_event_times)):
    _event_time = _event_times[i]
    if _event_time <= _interval_num:
        _num_events += 1
    else:
        _interval_nums.append(_interval_num)
        _num_events_in_interval.append(_num_events)

        print(str(_interval_num) +',' + str(_num_events))

        _interval_num += 1

        _num_events = 1

print(statistics.mean(_num_events_in_interval))

fig = plt.figure()
fig.suptitle('Number of events occurring in consecutive intervals in a simulated Poisson process')
plt.bar(_interval_nums, _num_events_in_interval)
plt.xlabel('Interval number')
plt.ylabel('Number of events')
#plt.savefig('/mnt/c/Users/aleja/Dropbox/Phd_Degree/Intership/Kubernetes_Fault_Tolerant/Scheduling_code/My_Scheduler/deployment/scenario_1/Consecutive_intervals.png')
plt.savefig('/mnt/c/Users/user/Dropbox/Phd_Degree/Intership/Kubernetes_Fault_Tolerant/Scheduling_code/My_Scheduler/deployment/scenario_1/Consecutive_intervals.png')

