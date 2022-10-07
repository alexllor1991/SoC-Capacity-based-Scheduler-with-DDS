LIMIT_OF_RECORDS = 1000

NUMBER_OF_CORE =  16 #20 

TOTAL_CPU_CLUSTER = NUMBER_OF_CORE * 1000 # value expressed in milli units of CPU

TOTAL_MEMORY_CLUSTER =  31993856 # 52407392  value expressed in Ki

NUMBER_NODES_CLUSTER = 4

MASTER_NAME = "kubernetes-control-plane" #"kind-control-plane" 

DEPLOY_IN_MASTER = True

DEPLOY_USING_NATIVE_SCHEDULER = False

MIX_METRICS = True

# refresh interval in seconds
TIME_INTERVAL = 3

TIME_INTERVAL_MULTIDEPLOYMENTS = 5

# estimation checking time in seconds
ESTIMATION_CHECKING_TIME = 200

LOG_FILE = '/tmp/usage.log'

MIN_PROCESS_CAPACITY = 500  #value in MIPS (Million of Instruction per Second) taken from paper Q-learning based dynamic task scheduling for energy-efficient cloud computing

MAX_PROCESS_CAPACITY = 3000  #value in MIPS (Million of Instruction per Second) taken from paper Q-learning based dynamic task scheduling for energy-efficient cloud computing

MIN_SOC_NODES = 30  #minimum percentage of SOC in a node before starting a migration process

PRIORITY_LIST_CRITERIA = 2  # if 0 the priority list is sorted by taking into account the service deadline. 
                            # if 1 the priority list is sorted by taking into account the service priority.
                            # if 2 the priority list is sorted by taking into account a dynamic rank 

SCHEDULING_CRITERIA = 2  # if 0 the scheduling criteria is a mixed relation between CPU and SOC
                         # if 1 the scheduling criteria takes into account the SOC estimation
                         # if 2 the scheduling criteria takes into account the CPU usage
                         # if 3 the scheduling criteria takes into account the best processing times in the nodes

delta_1 = 0.7 # weight in ranking priority queue (values between 0 and 1) 

alfa_1 = 0.5  # weight in ranking node (values between 0 and 1)

RMSE = 3

SOC_SENSOR = False