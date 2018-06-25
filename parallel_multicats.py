#!usr/bin/env python3

import argparse
# import os
import time
import glob
import signal
import itertools as it
from concurrent.futures import ProcessPoolExecutor
from random import random
from bisect import bisect
from csv import reader
from collections import Counter
from subprocess import Popen


def multiple_file_types(*patterns):
    return it.chain.from_iterable(glob.iglob(pattern) for pattern in patterns)


def first_file(ftype):
    # TODO: use this
    """ Finds the first file in the current directory
    of the specified type
    """
    type_relationship = {
        'ts': ['*.ts', '*.mpg'],
        'csv': ['*.csv'],
    }
    try:
        return next((filename for filename in multiple_file_types(type_relationship[ftype])))
    except StopIteration:
        raise FileNotFoundError(
            'no transport stream files found in current directory')


class WFileList(list):
    def __init__(self):
        self.choices = Counter()

    def append(self, item):
        """Overridden append that checks
        for tuple type and sorts members.
        :param item: object to add to list
        """
        if not isinstance(item, tuple):
            raise TypeError(f'must be tuple')
        super().append(item)
        return sorted(self)

    def elect(self):
        """Weighted elect of file based
        on weight assigned in manifest file.
        Uses bisect to assign probability regions
        and a random integer choice, meaning the
        choice is more likely to fall within
        larger regions.
        """
        if len(self) == 0:
            raise IndexError('no files')
        values, weights = zip(*self)
        total = 0
        cum_weights = []
        for w in weights:
            total += int(w)
            cum_weights.append(total)
        x = random() * total
        i = bisect(cum_weights, x)
        self.choices[values[i]] += 1
        return values[i]

    def __repr__(self):
        return 'List of files and their corresponding weights that elects files to stream, \
        taking their weight into account.'


def read_manifest(manifest_file: str):
    """Read the manifest file of filenames and weights
    and add to WFileList.
    :param manifest_file: name of manifest CSV file
    """
    # TODO: add handling for bad file writing;
    #   - formatting errors (space after comma breaks?)
    #   - check all files stated in manifest are accessible
    #   - (filename, weight), not (weight, filename) --> or, could sort each row when adding, by sorted(item, key=lambda item: type==str) or something like that -
    # https://stackoverflow.com/questions/34756863/python-sort-different-types-in-list good place to start
    # also https://www.peterbe.com/plog/in-python-you-sort-with-a-tuple
    if not manifest_file.endswith('.csv'):
        raise FileNotFoundError('manifest file must be in CSV format')
    weighted_files = WFileList()
    with open(manifest_file, 'r') as csvfile:
        for row in reader(csvfile):
            weighted_files.append(tuple(row))
        csvfile.close()
    return weighted_files


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
    aux_file = ts_file[:ts_file.index('.')] + '.aux'
    if not glob.glob(aux_file):
        print('No corresponding .aux file found;\nIngesting ts file:\n')
        print(
            'ingests -p {pcr_pid} {ts_file}'.format(pcr_pid=str(pcr_pid), ts_file=ts_file))
        Popen(['ingests', '-p', str(pcr_pid), ts_file])
        print('\n')
    return


def build_execution_string(file_choice: str, cip: str, cport: int, bip: str=None, bport: int=None) -> str:
    flags = " ".join(parser.flags)
    execution_string = 'multicat {flags} {fl} '.format(
        flags=flags, fl=file_choice) + '{cip}:{cport}'.format(cip=cip, cport=str(cport))
    additions = {
        parser.bip: '@{bip}'.format(bip=bip),
        parser.bport: ':{bport}'.format(bport=str(bport)),
        # multicat_options: '/{multicat_options}'.format(multicat_options=multicat_options)
    }
    for element, string_addition in additions.items():
        if element is not None and element != '' and element != 0:
            execution_string += string_addition
    execution_string += '\n\n'
    return execution_string


def build_execution_args(file_choice: str, cip: str, cport: int, bip: str=None, bport: int=None) -> list:
    execution_args = ['multicat']
    for flag in parser.flags:
        execution_args.append(flag)
    execution_args.append(file_choice)
    execution_args.append('{cip}:{cport}'.format(cip=cip, cport=str(cport)))
    additions = {
        parser.bip: '@{bip}'.format(bip=bip),
        parser.bport: ':{bport}'.format(bport=str(bport)),
        # multicat_options: '/{multicat_options}'.format(multicat_options=multicat_options)
    }
    for element, string_addition in additions.items():
        if element is not None and element != '' and element != 0:
            execution_args.append(string_addition)
    return execution_args


def multicat_thread(multicat_values: list):
    """ Run multicat in a process thread with above values
    :param multicat_values: list of values to pass to multicat
    """
    thread_no, ts_file, pcr_pid, ms, flags, \
        cip, cport, bip, bport = multicat_values
    try:
        ingest_ts(pcr_pid, ts_file)
        output_str = 'Thread no: {}'.format(
            thread_no) if thread_no == 1 else '\nThread no: {}'.format(thread_no)
        output_str += '\nUsing values:'
        output_str += '\n\tts file = {}'.format(ts_file)
        output_str += '\n\tpcr pid = {}'.format(pcr_pid)
        output_str += '\n\tthread count = {}'.format(TOTAL_THREADS)
        output_str += '\n\tconnect ip address = {}'.format(cip)
        output_str += '\n\tinitial connect port = {}'.format(CONNECT_PORT)
        output_str += '\n\tbind ip address = {}'.format(bip)
        output_str += '\n\tinitial bind port = {}'.format(BIND_PORT)
        output_str += '\n\tmilliseconds stagger = {}'.format(int(ms * 1000))
        output_str += '\n\tmulticat flags = {}\n'.format(flags)
        print(output_str)
        # <connect address>:<connect port>@<bind address>:<bind port>/<options>
        print('Running multicat:\n\n\t' +
              build_execution_string(ts_file, cip, cport, bip=bip, bport=bport))

        # run multicat, either in loop or not
        if parser.loop:
            while True:
                Popen(build_execution_args(
                    ts_file, cip, cport, bip=bip, bport=bport))
        else:
            Popen(build_execution_args(ts_file, cip, cport, bip=bip, bport=bport))

    except Exception as e:
        print('error')
        print(str(e))


def main():
    global parser
    parser = argparse.ArgumentParser()
    group = parser.add_mutually_exclusive_group(required=True)
    # these are the flags for this script
    group.add_argument(
        '--file', '-f', help='Name of transport stream file to use (defaults to first found in directory).')
    group.add_argument('--manifest', '-m',
                       help='Name of manifest file containing list of ts files and corresponding weights.')
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

    ###### Start Manifest file processing #########

    if parser.manifest:
        mf = parser.manifest
        weighted_files = read_manifest(mf)

    ###### End of Manifest file processing #########

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
    # file_choice = weighted_files.elect() if parser.manifest else parser.file
    mc_flag_additions = {
        # TODO: this is wrong - need to have an xml output for each ts file, not the manifest
        # '-T': '-T {xml_output}'.format(xml_output=file_choice.replace(".ts", ".xml")),
        '-t': '-t {ttl}'.format(ttl=parser.ttl),
        '-u': '-u {rtp}'.format(rtp=parser.RTP),
    }
    for i, flag in enumerate(parser.flags):
        parser.flags[i] = '-' + flag
        for k, v in mc_flag_additions.items():
            if parser.flags[i] == k:
                parser.flags[i] = v

    ###### End of Multicat flag processing #########

    global TOTAL_THREADS, CONNECT_PORT, BIND_PORT
    # used in outputting info
    CONNECT_IP = parser.ip
    CONNECT_PORT = parser.port
    BIND_IP = parser.bip
    BIND_PORT = parser.bport
    TOTAL_THREADS = parser.threads

    # ms --> s
    parser.ms /= 1000

    # Generate threads with multicat
    with ProcessPoolExecutor(max_workers=TOTAL_THREADS) as pool:
        futures = []
        thread_no = TOTAL_THREADS
        while parser.threads > 0:
            thread_no = TOTAL_THREADS - parser.threads + 1
            time.sleep(parser.ms)
            # elect a file based on weightings if using manifest (if not, use specified file)
            file_choice = weighted_files.elect() if parser.manifest else parser.file
            futures.append(pool.submit(multicat_thread, [
                thread_no, file_choice, parser.pid, parser.ms, parser.flags,
                CONNECT_IP, CONNECT_PORT, BIND_IP, BIND_PORT]))
            if parser.incr_port:
                CONNECT_PORT += 1
            if parser.incr_ip:
                CONNECT_IP = increment_ip(CONNECT_IP)
            parser.threads -= 1
            thread_no = TOTAL_THREADS

    if TOTAL_THREADS > 1 and parser.manifest:
        print('\n\nDistribution of file choices:\n')
        for k, v in weighted_files.choices.items():
            print('\t', k, 'was selected', v, 'times\n')


if __name__ == '__main__':
    main()
