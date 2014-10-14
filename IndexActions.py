#Imports and some thread classes for the parallelizing the SQL pipeline 
#where optimal
import pymysql as mdb
import time
import datetime
import Queue
import threading
import time
import sys
import copy
from pipelineConfig import ActionList, threadList, start, end
from pipelineUtilities import safeExecute 

con = mdb.connect('localhost', 'root', '', 'strikedbj3')
cur = con.cursor()

for action in ActionList:
    COMMAND = """ALTER TABLE  """+action+""" ADD KEY (time)"""
    print(COMMAND)
    with con:
        cur.execute(COMMAND)

    COMMAND = """ALTER TABLE """+action+""" ADD KEY (id)"""
    print(COMMAND)
    with con:
        cur.execute(COMMAND)

    COMMAND = """ALTER TABLE  """+action+""" ADD KEY (id,time)"""
    print(COMMAND)
    with con:
        cur.execute(COMMAND)

    COMMAND = """ALTER TABLE  """+action+""" ADD KEY (time,id)"""
    print(COMMAND)
    with con:
        cur.execute(COMMAND)
