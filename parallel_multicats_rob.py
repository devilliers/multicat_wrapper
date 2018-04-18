import argparse
import os
import time
import glob
import itertools as it
from concurrent.futures import ProcessPoolExecutor
from typing import List


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


parser = argparse.ArgumentParser()
parser.add_argument('--file', '-f', help='Name of transport stream file to use (defaults to first found in directory).',
                    default=first_ts_file())
parser.add_argument('--pid', help='PCR PID of ts file.',
                    type=int, default=33)
parser.add_argument(
    '--threads', '-t', help='Thread count (instances of multicat).', default=1, type=int)
parser.add_argument(
    '--ip', '-i', help='Target IPv4 address (of encoder).', default='0.0.0.0')
parser.add_argument(
    '--port', '-p', help='Starting target port number.', type=int, default=5001)
parser.add_argument(
    '--ms', help='Milliseconds to stagger launching each instance of multicat by.', type=int, default=500)
parser.add_argument('--incr_ip', action='store_true',
                    help='Set last number in target IPv4 address to increment with each thread.')
parser.add_argument('--incr_port', action='store_true',
                    help='Set last number in target IPv4 address to increment with each thread.')
parser.add_argument(
    '--flags', nargs='+',
    help='''Flags to pass to multicat (just use the letters themselves, without "-");
    run ```$ multicat --help``` for info on flags.''',
    default='u U'.split(), choices=[
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

# process flags and add any necessary info;
# done like this for easy addition of more flags
flag_additions = {
    '-T': f'-T {parser.file.replace(".ts", ".xml")}',
}
for i, flag in enumerate(parser.flags):
    parser.flags[i] = '-' + flag
    for k, v in flag_additions.items():
        if parser.flags[i] == k:
            parser.flags[i] = v

# used in outputting info
port_target = parser.port
ip_target = parser.ip
TOTAL_THREADS = parser.threads

# # print out script config
# print(f"""Using values:
#     ts file = {parser.file}
#     pcr pid = {parser.pid}
#     thread count = {parser.threads}
#     target ip address = {parser.ip}
#     initial port number = {port_target}
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
    aux_file = parser.file[:parser.file.index('.')] + '.aux'
    if not glob.glob(aux_file):
        print('Ingesting ts file...')
        os.system(f'ingests -p {pcr_pid} {ts_file}')
        print('\n')
    return


def multicat_thread(multicat_values: List):
    """ Run multicat in a process thread with above values
    :param details: 3-tuple of values to pass to multicat
    """
    global TOTAL_THREADS
    thread_no, ts_file, pcr_pid, ip_target, flags, ms, port_target = multicat_values
    try:
        ingest_ts(pcr_pid, ts_file)
        print(f"""Thread no: {thread_no}
Using values:
    ts file = {ts_file}
    pcr pid = {pcr_pid}
    thread count = {TOTAL_THREADS}
    target ip address = {ip_target}
    initial port number = {port_target}
    milliseconds stagger = {int(ms * 1000)}
    multicat flags = {flags}\n""")
        print('Running multicat:\n\n\t',
              f'multicat -t 20 {" ".join(flags)} {ts_file} {ip_target}:{str(port_target)}@10.10.111.2\n')
        os.system(
            f'multicat -t 20 {" ".join(flags)} {ts_file} {ip_target}:{str(port_target)}@10.10.111.2')
        print('\n')
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
                       thread_no, parser.file, parser.pid, ip_target, parser.flags, parser.ms, port_target]))
        if parser.incr_port:
            port_target += 1
        if parser.incr_ip:
            ip_target = increment_ip(ip_target)
        parser.threads -= 1
        thread_no = TOTAL_THREADS
