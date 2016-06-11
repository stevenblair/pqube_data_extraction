import os
import csv
import re
import datetime
import calendar
import time
from tables import *
import numpy as np

HARMONICS_DATA_FILENAMES = [
    'L1-N(V) Harmonics',
    'L2-N(V) Harmonics',
    'L3-N(V) Harmonics',
    'L1(A) Harmonics',
    'L2(A) Harmonics',
    'L3(A) Harmonics'
]

# re-open, sort the table, and check for inconsistent timestamps
def resort_tables(filename):
    t0 = time.time()
    # open database file
    f = open_file(filename + '.h5', mode='r+')#, driver="H5FD_CORE")
    monitor_tables = f.root

    t1 = time.time()

    for node in monitor_tables:
        table = node._f_get_child('readout')
        table.copy(newname='readout_sorted', sortby=table.cols.date, propindexes=True, overwrite=True)

        table = node._f_get_child('readout')
        table.remove()

        table = node._f_get_child('readout_sorted')
        table.rename('readout')

        #table = table.readSorted(sortby=table.cols.date)

        # verify timestamps are sorted: are increasing in order
        values = [row['date'] for row in table]
        num = len(values)
        print node._v_title, 'rows:', num
        for i in range(0, num):
            # check for 'isolated' initial value
            if 'Harmonics' in filename and i == 0 and num > 1:
                if abs(values[i + 1] - values[i]) != 60 * 15:
                    print 'changing earliest_date', values[i + 1], values[i], table._v_parent._v_attrs.earliest_date
                    table._v_parent._v_attrs.earliest_date = values[i + 1]

            # list bounds check
            if i == num - 1:
                continue
            if values[i] > values[i + 1]:
                print '  inconsistent timestamp', i, datetime.datetime.utcfromtimestamp(values[i]), datetime.datetime.utcfromtimestamp(values[i + 1])

    t2 = time.time()

    print 'time to sort file:', t2 - t1, 'time to open table:', t1 - t0

    # close and flush the data file
    f.close()


# resort_tables(MONITORING_DATA_FILENAME)
for harm_filename in HARMONICS_DATA_FILENAMES:
    resort_tables(harm_filename)