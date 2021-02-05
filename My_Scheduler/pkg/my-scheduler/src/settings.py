LIMIT_OF_RECORDS = 1000

NUMBER_OF_CORE = 20 # 12

TOTAL_CPU_CLUSTER = NUMBER_OF_CORE * 1000 # value expressed in milli units of CPU

TOTAL_MEMORY_CLUSTER = 52407392 # 26153684 value expressed in Ki

NUMBER_NODES_CLUSTER = 4

MIX_METRICS = True

# refresh interval in seconds
TIME_INTERVAL = 3

LOG_FILE = '/tmp/usage.log'

MIN_PROCESS_CAPACITY = 500  #value in MIPS (Million of Instruction per Second) taken from paper Q-learning based dynamic task scheduling for energy-efficient cloud computing

MAX_PROCESS_CAPACITY = 3000  #value in MIPS (Million of Instruction per Second) taken from paper Q-learning based dynamic task scheduling for energy-efficient cloud computing

PRIORITY_LIST_CRITERIA = 1  # if 0 the priority list is sorted by taking into account the service deadline. 
                            # if 1 the priority list is sorted by taking into account the service priority.
                            # if 2 the priority list is sorted dynamically by taking into account an equation 

SCHEDULING_CRITERIA = 2  # if 0 the scheduling criteria is a mixed relation between CPU and MEMORY usage
                         # if 1 the scheduling criteria takes into account the MEMORY usage
                         # if 2 the scheduling criteria takes into account the CPU usage
                         # if 3 the scheduling criteria takes into account the best processing times in the nodes

delta_1 = 0.7

delta_2 = 0.3