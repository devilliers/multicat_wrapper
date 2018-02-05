import sys
import os

PCR_PID = '33'
try:
    TS_FILE = sys.argv[1] if sys.argv[1] and sys.argv[1].endswith(
        '.ts') else 'BT_Barker-Promo-HDp25_5MBps.ts'
except IndexError:
    TS_FILE = 'BT_Barker-Promo-HDp25_5MBps.ts'

IP_ADDR_TARGET = '0.0.0.0'
PORT_TARGET = '5000'

try:
    os.system(f'ingests -p {PCR_PID} {TS_FILE}')
    os.system(f'multicat -X -U {TS_FILE} {IP_ADDR_TARGET}:{PORT_TARGET}')
except Exception as e:
    print(str(e))
