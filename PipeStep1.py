# coding: utf-8

# In[1]:

#!/usr/bin/python

#Imports and some thread classes for the parallelizing the SQL pipeline 
#where optimal
import pymysql as mdb
import time
import datetime
import Queue
import threading
import time
import sys

import ActionList from pipelineConfig
import threadList from pipelineConfig
import safeExecute from pipelineUtilities

con = mdb.connect('localhost', 'root', '', 'strikedbj3')
cur = con.cursor()


#We need to create a rollup period table to assist in analysis, if the rollup table doesn't already
#exist.  The table will allow us to query time intervals of interest for users based on user times.

# In[5]:

with con:
    COMMAND = "SELECT * FROM rollup LIMIT 1"
    tableExists = False
    try:
        cur.execute(COMMAND)
        tableExists = True 
    except:
        tableExists = False

    print tableExists

    if not tableExists:
        #Make roll up periods for users from 2014-01-01 to 2014-07-20 based off of modeanalytics scheme
        COMMAND = "CREATE TABLE rollup ( period_id INT, time_id DATETIME, start DATETIME, end DATETIME)"   
        cur.execute(COMMAND)

        today = datetime.date(2014,1,1)
        end = datetime.date(2014,7,20)
        while (today < end):

            d28 = today + datetime.timedelta(days=28)
            period_id = "1028"
            COMMAND = "INSERT INTO rollup VALUES("+period_id+", '"+str(today)+"', '"+str(today)+"', '"+str(d28)+"')"
            cur.execute(COMMAND)

            d7 =today + datetime.timedelta(days=7)
            period_id = "1007"
            COMMAND = "INSERT INTO rollup VALUES("+period_id+", '"+str(today)+"', '"+str(today)+"', '"+str(d7)+"')"
            cur.execute(COMMAND)

            d2 =today + datetime.timedelta(days=2)
            period_id = "1002"
            COMMAND = "INSERT INTO rollup VALUES("+period_id+", '"+str(today)+"', '"+str(today)+"', '"+str(d2)+"')"
            cur.execute(COMMAND)

            d1 =today + datetime.timedelta(days=1)
            period_id = "1001"
            COMMAND = "INSERT INTO rollup VALUES("+period_id+", '"+str(today)+"', '"+str(today)+"', '"+str(d1)+"')"
            cur.execute(COMMAND)

            d28 = today - datetime.timedelta(days=28)
            period_id = "2028"
            COMMAND = "INSERT INTO rollup VALUES("+period_id+", '"+str(today)+"', '"+str(d28)+"', '"+str(today)+"')"
            cur.execute(COMMAND)

            d7 =today - datetime.timedelta(days=7)
            period_id = "2007"
            COMMAND = "INSERT INTO rollup VALUES("+period_id+", '"+str(today)+"', '"+str(d7)+"', '"+str(today)+"')"
            cur.execute(COMMAND)

            d2 =today - datetime.timedelta(days=2)
            period_id = "2002"
            COMMAND = "INSERT INTO rollup VALUES("+period_id+", '"+str(today)+"', '"+str(d2)+"', '"+str(today)+"')"
            cur.execute(COMMAND)

            d1 =today - datetime.timedelta(days=1)
            period_id = "2001"
            COMMAND = "INSERT INTO rollup VALUES("+period_id+", '"+str(today)+"', '"+str(d1)+"', '"+str(today)+"')"
            cur.execute(COMMAND)
            today = today + datetime.timedelta(days=1)


# In[6]:

#Lets create unioned table of all actions
COMMAND = "DROP TABLE IF EXISTS Activity"
with con:
    cur.execute(COMMAND)
    
COMMAND = """
CREATE TABLE Activity
SELECT *
FROM 
(
SELECT id, time, plan 
FROM EdUse
UNION
SELECT id, time, plan 
FROM vPrice
UNION 
SELECT id, time, plan
FROM LandAn
) temp;
"""
with con:
    cur.execute(COMMAND)


# In[7]:

#Lets get the user start time
COMMAND = "DROP TABLE IF EXISTS StartTimes"
with con:
    cur.execute(COMMAND)
    
COMMAND = """
CREATE TABLE StartTimes
SELECT id, min(time) 
FROM Activity
GROUP BY id
"""
with con:
    cur.execute(COMMAND)


# In[8]:

#The Activity table is huge O(10) million rows. 
#Let's reduce the action resolution to days as we will 
#need this in repeated queries;
COMMAND = "DROP TABLE IF EXISTS ActiveUsers"
with con:
    cur.execute(COMMAND)

COMMAND = """
CREATE TABLE ActiveUsers 
Select DISTINCT DATE(a.time) as day, id, plan  
FROM Activity a 
ORDER BY 1,2;
"""
with con:
    cur.execute(COMMAND)


# In[9]:

#Now we know the active users by day, and can conduct queries 
#related to them
#This should hopefully also allow some parallization

#For each user on each day we want to know whether they deactivate or not.

#We can start by constructing a reduced Activity table just for the users
#And then perform more complex operations on the reduced table. 


# In[10]:

# In[11]:

exitFlag = 0
class myThreadWCU3(threading.Thread):
    def __init__(self, threadID, name, q):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.name = name
        self.q = q
        self.con = mdb.connect('localhost', 'root', '', 'strikedbj3')
        self.cur = self.con.cursor()
    def run(self):
        print "Starting " + self.name
        while not exitFlag:
            queueLock.acquire()
            if not workQueue.empty():
                code, day = self.q.get()
                queueLock.release()
                
                #Create reduced activity table
                COMMAND = "DROP TABLE IF EXISTS Act_"+str(code)
                safeExecute(COMMAND,self.con,self.cur,workQueue)
                
                COMMAND = """
                    CREATE TABLE Act_"""+str(code)+"""
                    SELECT day, id, plan 
                    FROM ActiveUsers 
                    WHERE id in(
                        SELECT id FROM ActiveUsers 
                        WHERE day ='"""+str(day)+"""')"""
                safeExecute(COMMAND,self.con,self.cur,workQueue)        
                
                #Now the complicated query on a smaller table
                COMMAND ="DROP TABLE IF EXISTS WCU3_"+str(code)
                safeExecute(COMMAND,self.con,self.cur,workQueue)
                COMMAND ="""
                CREATE TABLE WCU3_"""+str(code)+"""
                    SELECT 
                        """+str(day)+""" AS date ,
                        temp.e_id AS id , 
                        MIN(temp.eTime) AS minTime,
                    CASE 
                      WHEN temp.ee_id IS NOT NULL THEN 0
                        ELSE 1
                        END AS 'churned',
                        temp.plan AS plan
                      FROM 
                      (
                      SELECT 
                          e.id AS e_id, 
                          ee.id AS ee_id, 
                          e.day AS eTime, 
                          ee.day AS eeTime,
                          e.plan AS plan
                      FROM Act_"""+str(code)+""" e
                      LEFT JOIN Act_"""+str(code)+""" ee
                        ON e.id = ee.id
                        AND ee.day > """+str(day)+""" 
                        AND ee.day < """+str(day)+""" + INTERVAL 28 day
                        ) AS temp
                     GROUP BY 1,2
                     ORDER BY 1
                     """
                
                print self.name+" Executing:"+COMMAND
                safeExecute(COMMAND,self.con,self.cur,workQueue)
            else:
                queueLock.release()
            time.sleep(1)
        print "Exiting " + self.name


# In[ ]:

#Execute parallel table construction
queueLock = threading.Lock()
workQueue = Queue.Queue(201)
threads = []
threadID = 1
exitFlag = 0
# Create new threads
for tName in threadList:
    thread = myThreadWCU3(threadID, tName, workQueue)
    thread.start()
    threads.append(thread)
    threadID += 1
    
queueLock.acquire()
# Fill the queue
i = 0; 
today = datetime.date(2014,1,1)
end = datetime.date(2014,7,20)
while (today < end):
    workQueue.put([i,today])
    today = today + datetime.timedelta(days=1)
    print "filling queue", today
    i += 1

queueLock.release()

# Wait for queue to empty
while not workQueue.empty():
    pass
# Notify threads it's time to exit
exitFlag = 1
# Wait for all threads to complete
for t in threads:
    t.join()
print "Exiting Main Thread"


# In[ ]:

# Do the union 
