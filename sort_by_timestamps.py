import calendar
import datetime
import tables
import time


t0 = time.time()

# open database file
f = tables.open_file('monitoring-data.h5', mode='r+')#, driver="H5FD_CORE")
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

    # verify timestamps are sorted - are increasing in order
    values = [row['date'] for row in table]
    num = len(values)
    print node._v_title, 'rows:', num
    for i in range(0, num):
        if i == num - 1:
            continue
        if values[i] > values[i + 1]:
            print '  inconsistent timestamp', i, datetime.datetime.utcfromtimestamp(values[i]), datetime.datetime.utcfromtimestamp(values[i + 1])


t2 = time.time()

print t2 - t1, t1 - t0