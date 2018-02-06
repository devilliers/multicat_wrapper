import argparse
import os
from concurrent.futures import ProcessPoolExecutor  # , as_completed
from typing import List
import time
# from itertools import chain


def first_ts_file():
    """ Finds the first .ts
        file in the current directory
    """
    return next((f for f in os.listdir(os.getcwd()) if f.endswith('.ts'))) or 'no .ts files found'


parser = argparse.ArgumentParser()
parser.add_argument('--file', '-f', help='name of .ts file (defaults to first found in directory)',
                    default=first_ts_file())
parser.add_argument('--pid', help='PCR PID of ts file',
                    type=int, default=33)
parser.add_argument(
    '--threads', '-t', help='thread count (instances of multicat)', default=10, type=int)
parser.add_argument(
    '--ip', '-i', help='target ip address (of encoder)', default='0.0.0.0')
parser.add_argument(
    '--port', '-p', help='starting target port number', type=int, default=5000)
parser.add_argument(
    '--ms', help='milliseconds to stagger launching each instance of multicat by', type=int, default=500)
parser.add_argument(
    '--flags', nargs='+', help='flags to pass to multicat (just use the letters themselves, without "-"); run ```$ multicat --help``` for info on flags',
    default='u U', choices=[
        'X',
        'T',
        'f',
        'p',
        'C',
        'P',
        's',
        'n',
        'k',
        'd',
        'a',
        'r',
        'O',
        'S',
        'u',
        'U',
        'm',
        'R',
        'w',
    ])

parser = parser.parse_args()

# make multicat flags into actual flags
for i, flag in enumerate(parser.flags):
    parser.flags[i] = '-' + flag

# used in outputting info
port_target = parser.port
TOTAL_THREADS = parser.threads

# print out script config
print(f"""Using values:
    ts file = {parser.file}
    pcr pid = {parser.pid}
    thread count = {parser.threads}
    target ip address = {parser.ip}
    initial port number = {port_target}
    milliseconds stagger = {parser.ms}
    multicat flags = {parser.flags}\n""")

# s --> ms
parser.ms /= 1000


def multicat_thread(multicat_values: List):
    """ Run multicat in a process thread with above values
    :param details: 3-tuple of values to pass to multicat
    """
    global port_target, TOTAL_THREADS
    thread_no, ts_file, pcr_pid, ip_addr_target, flags, ms = multicat_values
    try:
        print('Ingesting .ts file...')
        os.system(f'ingests -p {pcr_pid} {ts_file}')
        print('\n')
        print(f"""Thread no: {thread_no}
Using values:
    ts file = {ts_file}
    pcr pid = {pcr_pid}
    thread count = {TOTAL_THREADS}
    target ip address = {ip_addr_target}
    initial port number = {port_target}
    milliseconds stagger = {ms}
    multicat flags = {flags}\n""")
        print('Running multicat...')
        os.system(
            f'multicat {" ".join(flags)} {ts_file} {ip_addr_target}:{str(port_target)}')
        print('\n')
    except Exception as e:
        print(str(e))
    port_target += 1


with ProcessPoolExecutor(max_workers=TOTAL_THREADS) as pool:
    futures = []
    thread_no = TOTAL_THREADS
    while parser.threads > 0:
        thread_no -= TOTAL_THREADS - parser.threads
        time.sleep(parser.ms)
        futures.append(pool.submit(multicat_thread, [
                       thread_no, parser.file, parser.pid, parser.ip, parser.flags, parser.ms]))
        parser.threads -= 1
        thread_no = TOTAL_THREADS
