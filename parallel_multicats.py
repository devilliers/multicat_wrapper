import argparse
import os
from concurrent.futures import ProcessPoolExecutor, as_completed
from typing import List
import time
# from itertools import chain


# initialise with default values
stream_count = 10
ip_addr_target = '0.0.0.0'
port_target = 5000
stagger_milliseconds = 500


def first_ts_file():
    """ Finds the first .ts
        file in the current directory
    """
    return next((f for f in os.listdir(os.getcwd()) if f.endswith('.ts'))) or 'no .ts files found'


parser = argparse.ArgumentParser()
parser.add_argument('--file', help='name of .ts file (defaults to first found in directory)',
                    default=first_ts_file())
parser.add_argument('--pid', help='PCR PID of ts file',  type=int, default=33)
parser.add_argument(
    '--threads', help='thread count (instances of multicat)', default=10, type=int)
parser.add_argument(
    '--ip', help='target ip address (of encoder)', default='0.0.0.0')
parser.add_argument(
    '--port', help='starting target port number', type=int, default=5000)
parser.add_argument(
    '--ms', help='milliseconds to stagger launching each instance of multicat by', type=int, default=500)

parser = parser.parse_args()

port_target = parser.port

# print out config
print(f"""Using values:
    ts file = {parser.file}
    pcr pid = {parser.pid}
    thread count = {parser.threads}
    target ip address = {parser.ip}
    initial port number = {port_target}
    milliseconds stagger = {parser.ms}""")

# s --> ms
parser.ms /= 1000


def multicat_thread(multicat_values: List):
    """ Run multicat in a process thread with above values
    :param details: 3-tuple of values to pass to multicat
    """
    global port_target, values_message
    ts_file, pcr_pid, stream_count, ip_addr_target = multicat_values
    try:
        # os.system(f'ingests -p {pcr_pid} {ts_file}')
        # os.system(
        #     f'multicat -u -U {ts_file} {ip_addr_target}:{str(port_target)}')
        print(f"""Using values:
    ts file = {parser.file}
    pcr pid = {parser.pid}
    thread count = {parser.threads}
    target ip address = {parser.ip}
    initial port number = {port_target}
    milliseconds stagger = {parser.ms}""")
    except Exception as e:
        print(str(e))
    port_target += 1
    return port_target - 1


# info on pool.submit() found here https://stackoverflow.com/questions/42074501/python-concurrent-futures-processpoolexecutor-performance-of-submit-vs-map#42081890
with ProcessPoolExecutor(max_workers=100) as pool:
    futures = []
    while parser.threads > 0:
        time.sleep(parser.ms)
        futures.append(pool.submit(multicat_thread, [
                       parser.file, parser.pid, parser.threads, parser.ip]))
        parser.threads -= 1
    print([f.result() for f in as_completed(futures)])
    # print(chain.from_iterable(f.result() for f in as_completed(futures)))
