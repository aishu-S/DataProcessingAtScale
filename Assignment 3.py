#!/usr/bin/python2.7
#
# Assignment3 Interface
#

import psycopg2
import os
import sys
import threading

# Donot close the connection inside this file i.e. do not perform openconnection.close()
def ParallelSort (InputTable, SortingColumnName, OutputTable, openconnection):
    #Implement ParallelSort Here.
    pres = openconnection.cursor()
    pres.execute("Select MIN(" + SortingColumnName + ") FROM " + InputTable + "")
    MinValue=pres.fetchone()[0]
    pres.execute("Select MAX(" + SortingColumnName + ") FROM " + InputTable + "")
    MaxValue=pres.fetchone()[0]
    interval_range = (MaxValue - MinValue)/5
    pres.execute("Select column_name,data_type FROM information_schema.columns WHERE table_name='" + InputTable + "'")
    sch = pres.fetchall()
    thrd=5

    for i in range(thrd):
        table_name = "range_part" + str(i)
        pres.execute("Create TABLE " + table_name + " (" +sch[0][0]+ " " +sch[0][1]+ ")")
        for j in range(1, len(sch)):
            pres.execute("Alter TABLE " + table_name + " Add COLUMN " + sch[j][0] + " " + sch[j][1] + ";")

    thread_vals = []

    for i in range(thrd):
        if i == 0:
            low = MinValue
            max_val = MinValue + interval_range
        else:
            low = max_val
            max_val = max_val + interval_range
        t = threading.Thread(target=insert_table1, args=(InputTable, SortingColumnName, i, low, max_val, openconnection))
        thread_vals.append(t)
        thread_vals[i].start()

    for p in range(thrd):
        thread_vals[i].join()

    pres.execute("Create TABLE " + OutputTable + " (" +sch[0][0]+ " " +sch[0][1]+ ")")

    for i in range(1, len(sch)):
        pres.execute("Alter TABLE " + OutputTable + " ADD COLUMN " + sch[i][0] + " " + sch[i][1] + ";")

    for i in range(thrd):
        q = "Insert into " + OutputTable + " SELECT * FROM " + "range_part" + str(i) + ""
        pres.execute(q)

def ParallelJoin (InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, OutputTable, openconnection):
    #Implement ParallelJoin Here.
    pres = openconnection.cursor()
    pres.execute("Select MIN(" + Table1JoinColumn + ") FROM " + InputTable1 + "")
    minv=pres.fetchone()[0]
    pres.execute("Select MIN(" + Table2JoinColumn + ") FROM " + InputTable2 + "")
    min_val=pres.fetchone()[0]
    pres.execute("Select MAX(" + Table1JoinColumn + ") FROM " + InputTable1 + "")
    maxv=pres.fetchone()[0]
    pres.execute("Select MAX(" + Table2JoinColumn + ") FROM " + InputTable2 + "")
    max_val=pres.fetchone()[0]
    if minv > min_val:
        min_range = min_val
    else:
        min_range = minv
    if maxv > max_val:
        max_range = maxv
    else:
        max_range = max_val
    interval_range = (max_range - min_range)/5
    pres.execute("Select column_name,data_type FROM information_schema.columns WHERE TABLE_NAME='" + InputTable1 + "'")
    sch1 = pres.fetchall()
    pres.execute("Select column_name,data_type FROM information_schema.columns WHERE TABLE_NAME='" + InputTable2 + "'")
    sch2 = pres.fetchall()
    pres.execute("Create TABLE " + OutputTable + " ("+sch1[0][0]+" "+sch2[0][1]+")")

    for i in range(1, len(sch1)):
        pres.execute("Alter TABLE " + OutputTable + " ADD COLUMN " + sch1[i][0] + " " + sch1[i][1] + ";")

    for i in range(len(sch2)):
        pres.execute("Alter TABLE " + OutputTable + " ADD COLUMN " + sch2[i][0] + "1" +" " + sch2[i][1] + ";")

    thrd=5

    for i in range(thrd):
            table1_name = "table1_range" + str(i)
            table2_name = "table2_range" + str(i)
            if i==0:
                low = min_range
                maxv = min_range + interval_range
            else:
                low = maxv
                maxv = maxv + interval_range
            if i == 0:
                pres.execute("Create TABLE " + table1_name + " As Select * From " + InputTable1 + " Where (" + Table1JoinColumn + " >= " + str(low) + ") And (" + Table1JoinColumn + " <= " + str(maxv) + ")")
                pres.execute("Create TABLE " + table2_name + " As Select * From " + InputTable2 + " Where (" + Table2JoinColumn + " >= " + str(low) + ") And (" + Table2JoinColumn + " <= " + str(maxv) + ")")
            else:
                pres.execute("Create TABLE " + table1_name + " As Select * From " + InputTable1 + " Where (" + Table1JoinColumn + " > " + str(low) + ") And (" + Table1JoinColumn + " <= " + str(maxv) + ")")
                pres.execute("Create TABLE " + table2_name + " As Select * From " + InputTable2 + " Where (" + Table2JoinColumn + " > " + str(low) + ") And (" + Table2JoinColumn + " <= " + str(maxv) + ")")
            output_range_table = "output_table" + str(i)
            pres.execute("Create TABLE " + output_range_table + " ("+sch1[0][0]+" "+sch2[0][1]+")")

            for j in range(1, len(sch1)):
                pres.execute("Alter TABLE " + output_range_table + " Add column " + sch1[j][0] + " " + sch1[j][1] + ";")

            for j in range(len(sch2)):
                pres.execute("Alter TABLE " + output_range_table + " Add column " + sch2[j][0] + "1" +" "+ sch2[j][1] + ";")

    thread_vals = []

    for i in range(thrd):
        t1 = threading.Thread(target=insert_tables2, args=(Table1JoinColumn, Table2JoinColumn, openconnection, i))
        thread_vals.append(t1)
        thread_vals[i].start()

    for a in range(thrd):
        thread_vals[i].join()

    for i in range(thrd):
        pres.execute("Insert into " + OutputTable + " Select * FROM output_table" + str(i))

    pres.close()


def insert_tables2(Table1JoinColumn, Table2JoinColumn, openconnection, i):
    pres=openconnection.cursor()
    query = "Insert into output_table" + str(i) + " Select * FROM table1_range" + str(i) + " INNER JOIN table2_range" + str(i) + " ON table1_range" + str(i) + "."  + Table1JoinColumn + "=" + "table2_range" + str(i) + "." + Table2JoinColumn + ";"
    pres.execute(query)
    pres.close()
    return

def insert_table1(InputTable, SortingColumnName, i, min, max, openconnection):
    pres=openconnection.cursor()
    TableName = "range_part" + str(i)
    if i == 0:
        q = "Insert into " + TableName + " Select * FROM " + InputTable + "  WHERE " + SortingColumnName + ">=" + str(min) + " AND " + SortingColumnName + " <= " + str(max) + " ORDER BY " + SortingColumnName + " ASC"
    else:
        q = "Insert into " + TableName + " Select * FROM " + InputTable + "  WHERE " + SortingColumnName + ">" + str(min) + " AND " + SortingColumnName + " <= " + str(max) + " ORDER BY " + SortingColumnName + " ASC"
    pres.execute(q)
    pres.close()
    return