import json
from enum import Enum
from kubernetes import client
import settings

class DataType(Enum):
    MEMORY = 0
    CPU = 1

memory_type_wage = \
    {
        'Ki': 1, 'K': 1,
        'Mi': 1000, 'M': 1000,
        'Gi': 1000000, 'G': 1000000,
        'Ti': 1000000000, 'T': 1000000000,
        'Pi': 1000000000000, 'P': 1000000000000,
        'Ei': 1000000000000000, 'E': 1000000000000000,
    }

cpu_type_wage = \
    {
        'm': 1, 'u': 1,
        'n': 0.000001,
    }

BASE_MEMORY_WAGE = memory_type_wage['Ki']

BASE_CPU_WAGE = cpu_type_wage['m']

class PodList(object):
    def __init__(self):
        self.items = []

class Pod(object):
    def __init__(self, metadata_, spec_, status_):
        """
        :param V1ObjectMeta metadata_:
        :param V1PodSpec spec_:
        :param V1PodStatus status_:
        :return:
        """
        if type(metadata_) is not client.models.v1_object_meta.V1ObjectMeta:
            raise str("Passed invalid type")
        if type(spec_) is not client.models.V1PodSpec:
            raise str("Passed invalid type")
        if type(status_) is not client.models.V1PodStatus:
            raise str("Passed invalid type")

        self.metadata = metadata_
        self.spec = spec_
        self.status = status_
        self.demanded_processing = None
        self.id = None
        self.service_id = None
        self.deadline = None
        self.running_time_task = None
        self.service_arrival_time = None
        self.rank = None
        self.priority = None
        self.usage = []
        self.is_alive = True

        # label set priority for this pod
        self.scheduling_criteria = None

    def __eq__(self, other):
        return self.metadata.name == other.metadata.name

    def to_dir(self):
        print( \
            {
                'Id': self.id, 
                'Name': self.metadata.name,
                'Demanded_processing': self.demanded_processing,
                'Running_time': self.running_time_task,
                'Deadline': str(self.deadline),
                'Service_arrival_time': str(self.service_arrival_time),
                'Priority': self.priority,
                'Scheduling_criteria': self.scheduling_criteria,
                'Rank': self.rank,
            })

    def fetch_usage(self):
        """
        Fetch usage data for pod from metrics server
        :return:
        """
        metrics_url = '/apis/metrics.k8s.io/v1beta1/namespaces/' \
                      + self.metadata.namespace + '/pods/' \
                      + self.metadata.name

        try:
            configuration = client.Configuration()
            api_client = client.ApiClient(configuration)
            response = api_client.call_api(metrics_url, 'GET', auth_settings = ['BearerToken'], response_type='json', _preload_content=None)
            resp = response[0].data.decode('utf-8')
            json_data = json.loads(resp)
        except Exception as e:
            if len(self.usage) <= 1:
                self.usage = []
            # self.usage.append({'cpu': 0, 'memory': 0})
            return int(str(e)[1:4])

        # Pod usage is sum of usage of all containers running inside it
        tmp_mem = 0
        tmp_cpu = 0

        for container in json_data['containers']:
            if not settings.MIX_METRICS:
                # when Pod is in Error state, it containers usage is returned as 0
                if container['usage']['memory'] != '0':
                    tmp_mem += self.parse_usage_data(container['usage']['memory'], DataType.MEMORY)
                if container['usage']['cpu'] != '0':
                    tmp_cpu += self.parse_usage_data(container['usage']['cpu'], DataType.CPU)

            elif settings.MIX_METRICS:
                # if container have specified req field use it instead of usage
                found = False
                tmp_cont = None

                for cont in self.spec.containers:
                    if cont.name == container['name'] and cont.resources.requests is not None:
                        tmp_cont = cont
                        found = True
                        break

                if found:
                    # there can be only one param specified in requests

                    if 'cpu' in tmp_cont.resources.requests:
                        tmp_cpu += self.parse_usage_data(tmp_cont.resources.requests['cpu'], DataType.CPU)
                    else:
                        if container['usage']['cpu'] != '0':
                            tmp_cpu += self.parse_usage_data(container['usage']['cpu'], DataType.CPU)

                    if 'memory' in tmp_cont.resources.requests:
                        tmp_mem += self.parse_usage_data(tmp_cont.resources.requests['memory'], DataType.MEMORY)
                    else:
                        if container['usage']['memory'] != '0':
                            tmp_mem += self.parse_usage_data(container['usage']['memory'], DataType.MEMORY)

                else:
                    if container['usage']['memory'] != '0':
                        tmp_mem += self.parse_usage_data(container['usage']['memory'], DataType.MEMORY)
                    if container['usage']['cpu'] != '0':
                        tmp_cpu += self.parse_usage_data(container['usage']['cpu'], DataType.CPU)

        if len(self.usage) > settings.LIMIT_OF_RECORDS:
            self.usage.pop(0)

        self.usage.append({'cpu': tmp_cpu, 'memory': tmp_mem})

        return 0

    @staticmethod
    def parse_usage_data(data_string, data_type):
        """
        Parse input stream to unified value for CPU and memory usage
        memory data string eg. 128974848, 129e6, 129M, 123Mi
        correct types Ei, Pi, Ti, Gi, Mi, Ki
        :param str data_string: string containing data to translate
        :param  DataType data_type: type of input data
        :return int/None: return value in Ki for memory and m for
            CPU, None if passed data was incorrect
        """
        if type(data_type) is not DataType:
            print('Passed invalid type')
            return None

        if len(data_string) == 0:
            print('Passed empty data string')
            return None

        if data_type == DataType.MEMORY:

            if data_string[-1].isalpha():

                if data_string[-2:].isalpha():
                    wage = memory_type_wage[data_string[-2:]] / BASE_MEMORY_WAGE
                    value = int(data_string[:-2])
                elif data_string[-1:].isalpha():
                    wage = memory_type_wage[data_string[-1:]] / BASE_MEMORY_WAGE
                    value = int(data_string[:-1])
                else:
                    return None

                return int(wage * value)

            else:
                if 'e' in data_string:
                    print('E notation not implemented yet')
                    raise Exception('Not implemented')
                    # TODO implement this

                return int(data_string)

        elif data_type == DataType.CPU:

            if data_string[-1].isalpha():
                wage = cpu_type_wage[data_string[-1]] / BASE_CPU_WAGE
                value = int(data_string[:-1])
                # for char in data_string[:-1]:
                #     print(char)
                #     if char.isalpha():
                #         return None
                # print(data_string[:-1])
                #return int(data_string[:-1])
                return float(wage * value)
            else:
                return None

    def get_usage(self):
        """
        Get usage calculated based on Pod statistics
        :return dict: dict('cpu': cpu_usage, 'memory': memory_usage)
        """
        sum_cpu = 0.0
        sum_mem = 0.0
        if len(self.usage) > 0:
            for entry in self.usage:
                sum_cpu += float(entry['cpu'])
                sum_mem += float(entry['memory'])
            avg_cpu = sum_cpu / len(self.usage)
            avg_mem = sum_mem / len(self.usage)
            return {'cpu': avg_cpu, 'memory': avg_mem}
        else:
            return {'cpu': 0, 'memory': 0}