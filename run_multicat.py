import sys
import os
from concurrent.futures import ProcessPoolExecutor
from typing import Tuple

# initialise with default values
TS_FILE = 'BT_Barker-Promo-HDp25_5MBps.ts'
PCR_PID = '33'
STREAM_COUNT = 1
IP_ADDR_TARGET = '0.0.0.0'
PORT_TARGET = '5000'

# set values to argv values
try:
    TS_FILE, PCR_PID, STREAM_COUNT = sys.argv[1:]
except ValueError:
    print(f'\nUsage: \n\tpython3.6 run_multicat.py TS_FILE PCR_PID STREAM_COUNT\n')
    accept_defaults = input(
        f'Use default values of TS_FILE={TS_FILE}, PCR_PID={PCR_PID} and STREAM_COUNT={STREAM_COUNT}? [y/n] ')
    if accept_defaults == 'n':
        print('\n')
        sys.exit()


def multicat_thread(multicat_values: Tuple):
    """ Run multicat in a process thread with above values
    :param details: 3-tuple of values to pass to multicat
    """
    TS_FILE, PCR_PID, STREAM_COUNT = multicat_values
    try:
        # os.system(f'ingests -p {PCR_PID} {TS_FILE}')
        # os.system(f'multicat -X -U {TS_FILE} {IP_ADDR_TARGET}:{PORT_TARGET}')
        print('running multicat')
    except Exception as e:
        print(str(e))


# make the Pool of workers
pool = ProcessPoolExecutor(max_workers=100)

iterator = [(TS_FILE, PCR_PID, STREAM_COUNT) for _ in range(50)]

# map function to each thread in the pool
results = pool.map(multicat_thread, iterator)
