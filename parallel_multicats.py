#!usr/bin/env python3

import argparse
import os
import time
import glob
import signal
import itertools as it
from concurrent.futures import ProcessPoolExecutor
from typing import List


def signal_handler(signal, frame):
    global interrupted
    interrupted = True


signal.signal(signal.SIGINT, signal_handler)


def multiple_file_types(*patterns):
    return it.chain.from_iterable(glob.iglob(pattern) for pattern in patterns)


def first_ts_file():
    """ Finds the first .ts
        file in the current directory
    """
    try:
        return next((filename for filename in multiple_file_types('*.ts', '*.mpg')))
    except StopIteration:
        raise FileNotFoundError(
            'no transport stream files found in current directory')


# TODO run indefinitely? --> look through MC docs, then google, then look in MC code
# TODO add rest of MC flags?


parser = argparse.ArgumentParser()

# these are the flags for this script
parser.add_argument('--file', '-f', help='Name of transport stream file to use (defaults to first found in directory).',
                    default=first_ts_file())
parser.add_argument('--pid', help='PCR PID of ts file.',
                    type=int, default=33)
parser.add_argument(
    '--threads', '-t', help='Thread count (instances of multicat).', default=1, type=int)
parser.add_argument(
    '--ip', '-i', help='Connect IPv4 address (of encoder).', default='0.0.0.0')
parser.add_argument(
    '--port', '-p', help='Starting connect port number.', type=int, default=5001)
parser.add_argument(
    '--bip', help='Bind IPv4 address (of encoder).', default='10.10.111.2')
parser.add_argument(
    '--bport', help='Starting bind port number.', type=int)
parser.add_argument(
    '--ms', help='Milliseconds to stagger launching each instance of multicat by.', type=int, default=500)
parser.add_argument('--incr_ip', action='store_true',
                    help='Set last number in connect IPv4 address to increment with each thread spawned.')
parser.add_argument('--incr_port', action='store_true',
                    help='Set connect port to increment with each thread spawned.')
parser.add_argument('--loop', '-l', action='store_true')
parser.add_argument('--RTP', type=str)
parser.add_argument('--ttl', type=int)

# these are the flags to pass to straight to Multicat, that would be used when
# running Multicat without this wrapper script. Those that allow a variable to be
# passed in are processed below.
parser.add_argument(
    '--flags', nargs='+',
    help='''Flags to pass to multicat (just use the letters themselves, without "-");
    run ```$ multicat --help``` for info on flags.''', default=[], choices=['X', 'T', 'f', 'p', 'C', 'P', 's', 'n',
                                                                            'k', 'd', 'a', 'r', 'O', 'S', 'u', 'U',
                                                                            'm', 'R', 'w', ])

parser = parser.parse_args()

###### Start Multicat flag processing #########

# if these are flags (like --ttl, for example) are used, the corresponding flags
# need to be passed to Multicat
flags_to_add_to_multicat = {
    parser.ttl: 't',
    parser.RTP: 'u'
}
for flag in flags_to_add_to_multicat:
    if flag:
        parser.flags.append(flags_to_add_to_multicat[flag])

# pass any flag variables through to multicat flags that use them
mc_flag_additions = {
    '-T': f'-T {parser.file.replace(".ts", ".xml")}',
    '-t': f'-t {parser.ttl}',
    '-u': f'-u {parser.RTP}'
}
for i, flag in enumerate(parser.flags):
    parser.flags[i] = '-' + flag
    for k, v in mc_flag_additions.items():
        if parser.flags[i] == k:
            parser.flags[i] = v

###### End of Multicat flag processing #########

# used in outputting info
CONNECT_IP = parser.ip
CONNECT_PORT = parser.port
BIND_IP = parser.bip
BIND_PORT = parser.bport
TOTAL_THREADS = parser.threads

# # print out script config
# print(f"""Using values:
#     ts file = {parser.file}
#     pcr pid = {parser.pid}
#     thread count = {parser.threads}
#     target ip address = {parser.ip}
#     initial port number = {CONNECT_PORT}
#     milliseconds stagger = {parser.ms}
#     multicat flags = {parser.flags}\n""")

# ms --> s
parser.ms /= 1000


def increment_ip(ip: str) -> str:
    """Increment the fourth num
    in in IPv4 addr
    :param ip: IPv4 addr
    """
    ip_parts = ip.split('.')
    ip_parts[3] = str(int(ip_parts[3]) + 1)
    return '.'.join(ip_parts)


def ingest_ts(pcr_pid: int, ts_file: str):
    """Ingest only if ts
    file hasn't already been ingested
    :param pcr_pid: pcr pid of ts_file
    :param ts_file: filename of ts file
    """
    print(ts_file)
    aux_file = parser.file[:parser.file.index('.')] + '.aux'
    if not glob.glob(aux_file):
        print('Ingesting ts file...')
        os.system(f'ingests -p {pcr_pid} {ts_file}')
        print('\n')
    return


def build_execution_string(cip: str, cport: int, bip: str=None, bport: int=None) -> str:
    execution_string = f'multicat {" ".join(parser.flags)} {parser.file} ' + \
        f'{cip}:{str(cport)}'
    additions = {
        parser.bip: f'@{bip}',
        parser.bport: f':{str(bport)}',
        # multicat_options: f'/{multicat_options}'
    }
    for element, string_addition in additions.items():
        if element is not None and element != '' and element != 0:
            execution_string += string_addition
    execution_string += '\n\n'
    return execution_string


def multicat_thread(multicat_values: List):
    """ Run multicat in a process thread with above values
    :param details: 3-tuple of values to pass to multicat
    """
    global TOTAL_THREADS, CONNECT_PORT, BIND_PORT
    thread_no, ts_file, pcr_pid, ms, flags, \
        cip, cport, bip, bport = multicat_values
    try:
        ingest_ts(pcr_pid, ts_file)
        print(f"""Thread no: {thread_no}
Using values:
    ts file = {ts_file}
    pcr pid = {pcr_pid}
    thread count = {TOTAL_THREADS}
    connect ip address = {cip}
    initial connect port = {CONNECT_PORT}
    bind ip address = {bip}
    initial bind port = {BIND_PORT}
    milliseconds stagger = {int(ms * 1000)}
    multicat flags = {flags}\n""")
        # <connect address>:<connect port>@<bind address>:<bind port>/<options>
        print('Running multicat:\n\n\t' +
              build_execution_string(cip, cport, bip=bip, bport=bport))
        if parser.loop:
            # interrupted = False
            while True:
                os.system(build_execution_string(
                    cip, cport, bip=bip, bport=bport))
                # supposedly a fix to not being able to CTRL+C
                # to exit, but doesn't seem to work...
                # if interrupted:
                #     print("Killing loop.")
                #     break
        else:
            os.system(build_execution_string(cip, cport, bip, bport))
    except Exception as e:
        print(str(e))


# Generate threads with multicat
with ProcessPoolExecutor(max_workers=TOTAL_THREADS) as pool:
    futures = []
    thread_no = TOTAL_THREADS
    while parser.threads > 0:
        thread_no = TOTAL_THREADS - parser.threads + 1
        time.sleep(parser.ms)
        futures.append(pool.submit(multicat_thread, [
                       thread_no, parser.file, parser.pid, parser.ms, parser.flags,
                       CONNECT_IP, CONNECT_PORT, BIND_IP, BIND_PORT]))
        if parser.incr_port:
            CONNECT_PORT += 1
        if parser.incr_ip:
            CONNECT_IP = increment_ip(CONNECT_IP)
        parser.threads -= 1
        thread_no = TOTAL_THREADS
