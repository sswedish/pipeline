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

#Lets create unioned table of all actions
COMMAND = "DROP TABLE IF EXISTS WCU3"
with con:
    cur.execute(COMMAND)
    
today = copy.copy(start)        
code = 0;
COMMAND = """
CREATE TABLE WCU3
SELECT *
FROM ( 
    SELECT date, id, minTime, churned, plan
    FROM WCU3_"""+str(code)

today = today + datetime.timedelta(days=1)
code += 1
while (today < end):
    today = today + datetime.timedelta(days=1)
    COMMAND +="""
                 UNION
                 SELECT date, id, minTime, churned, plan
                 FROM WCU3_"""+str(code)
    code +=1
COMMAND += """) temp; """
print COMMAND
with con:
    cur.execute(COMMAND)

COMMAND = """ALTER TABLE  WCU3 ADD KEY (date)"""
print(COMMAND)
with con:
    cur.execute(COMMAND)

COMMAND = """ALTER TABLE WCU3 ADD KEY (id)"""
print(COMMAND)
with con:
    cur.execute(COMMAND)

COMMAND = """ALTER TABLE  WCU3 ADD KEY (id,date)"""
print(COMMAND)
with con:
    cur.execute(COMMAND)

COMMAND = """ALTER TABLE  WCU3 ADD KEY (date,id)"""
print(COMMAND)
with con:
    cur.execute(COMMAND)
