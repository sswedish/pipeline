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

#create names for distinct threads to be used by program
#number specified by user in first command line argument#
threadList = []        
for i in range(int(sys.argv[1])):
    threadList += ["Thread-"+str(i)]

ActionList = ["EDUse","vPrice","LandAn","Ed20","Ed50",
              "ClickPub","CustFav","CustDom","CustSubDom",
              "CustFBIm","SEO"]

#Adds exception handling to execute command, was useful for 
#prototyping and debugging threads in ipython
def safeExecute(COMMAND,con,cur,que):
    with con:
        try:
            cur.execute(COMMAND)
        except:
            print "ERROR: occurred on command:"
            print COMMAND
            print "Emptying thread queue ..."
            que.queue.clear()


#Get connection for some initial table constuction
con = mdb.connect('localhost', 'root', '', 'strikedbj3')
cur = con.cursor()


#Create a rollup period table to assist in analysis, if the rollup table doesn't already
#exist.  The table will allow us to query time intervals of interest for users based on user times.
with con:
    COMMAND = "SELECT * FROM rollup LIMIT 1"
    tableExists = False
    try:
        cur.execute(COMMAND)
        tableExists = True 
    except:
        tableExists = False

    print tableExists
    #Make roll up periods for users from 2014-01-01 to 2014-07-20 based off of modeanalytics scheme
    if not tableExists:
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


#Create unioned table of all actions
COMMAND = "DROP TABLE IF EXISTS Activity"
with con:
    cur.execute(COMMAND)
    
COMMAND = """
CREATE TABLE Activity
SELECT * 
FROM 
(
SELECT id, time, plan 
FROM EDUse
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


#Create in parrallel a "master" table to identify active users on a given day
#and store static information about them label as churned if they went
#innactive in the next 28 days

#global status flag for threads
exitFlag = 0

#thread class for creating this table
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
                code, lower, upper = self.q.get()
                queueLock.release()
                COMMAND ="DROP TABLE IF EXISTS WCU3_"+str(code)
                safeExecute(COMMAND,self.con,self.cur,workQueue)
                COMMAND ="""
                CREATE TABLE WCU3_"""+str(code)+"""
                    SELECT 
                        DATE(drp.time_id) AS date ,
                        temp.e_id AS id , 
                        MIN(temp.eTime) AS minTime,
                    CASE 
                      WHEN temp.ee_id IS NOT NULL THEN 0
                        ELSE 1
                        END AS 'churned',
                        temp.plan AS plan
                      FROM rollup drp
                      LEFT JOIN (
                      SELECT 
                          e.id AS e_id, 
                          ee.id AS ee_id, 
                          e.time AS eTime, 
                          ee.time AS eeTime,
                          e.plan AS plan
                      FROM Activity e
                      LEFT JOIN Activity ee
                        ON e.id = ee.id
                        AND ee.time > e.time
                        AND ee.time < e.time + INTERVAL 28 day
                        ) AS temp
                        ON DATE(temp.eTime) = DATE(drp.time_id)
                        AND drp.period_id = 2007
                        AND DATE(drp.time_id) > """+lower+"""
                        AND DATE(drp.time_id) <= """+upper+"""
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
workQueue = Queue.Queue(10)
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
workQueue.put([1,"'2014-01-01'","'2014-02-01'"])
workQueue.put([2,"'2014-02-01'","'2014-03-01'"])
workQueue.put([3,"'2014-03-01'","'2014-04-01'"])
workQueue.put([4,"'2014-04-01'","'2014-05-01'"])
workQueue.put([5,"'2014-05-01'","'2014-06-01'"])
workQueue.put([6,"'2014-06-01'","'2014-07-01'"])
workQueue.put([7,"'2014-07-01'","'2014-07-21'"])

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
COMMAND = """ DROP TABLE IF EXISTS WCU3 """
with con:
    cur.execute(COMMAND)
    
COMMAND = """
    CREATE TABLE WCU3
        SELECT * FROM WCU3_1
        UNION
        SELECT * FROM WCU3_2
        UNION
        SELECT * FROM WCU3_3
        UNION
        SELECT * FROM WCU3_4
        UNION
        SELECT * FROM WCU3_5
        UNION
        SELECT * FROM WCU3_6
        UNION
        SELECT * FROM WCU3_7
  """
with con:
    cur.execute(COMMAND)


#Add to the table the start time of the user, defined as first time 
#they performed an action

COMMAND = "ALTER TABLE WCU3 ADD start DATETIME"
with con:
    try:
        cur.execute(COMMAND)
    except:
        print "Column must already exist"

COMMAND = """
    UPDATE WCU3 AS w
    LEFT JOIN (SELECT r.id AS id, 
                MIN(r.time) AS start
                FROM Activity AS r
                GROUP BY r.id) AS t
    ON w.id = t.id
    SET W.start = t.start
    """
with con:
    cur.execute(COMMAND)


#Setup for parrallel run to construct count tables
exitFlag = 0
debug = 0
class myThreadDeltaCounts(threading.Thread):
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
                action, delta = self.q.get()
                queueLock.release()
                COMMAND = "DROP TABLE IF EXISTS "+action+"_count"+str(delta)
                safeExecute(COMMAND,self.con,self.cur,workQueue)
                COMMAND = """
                    CREATE TABLE """+action+"""_count"""+str(delta)+"""
                    SELECT W.date AS time,
                           W.id AS id, 
                           COUNT(W.id) AS  count,
                           W.churned AS churned
                    FROM WCU3 AS W
                    LEFT JOIN """+action+""" AS e
                    ON W.id = e.id
                    AND DATE(e.time) < W.date"""
                if delta > 0:
                    COMMAND += " AND DATE(e.time) >= W.date - INTERVAL "+str(delta)+" day"
                COMMAND +="""    
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
workQueue = Queue.Queue(10)
threads = []
threadID = 1
exitFlag = 0
# Create new threads
for tName in threadList:
    thread = myThreadDeltaCounts(threadID, tName, workQueue)
    thread.start()
    threads.append(thread)
    threadID += 1
    
queueLock.acquire()
# Fill the queue
for delta in [0,1,2,7,28]:
    for action in ActionList:
        workQueue.put([action,delta])
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

#Now we need the last time an action occured. 
exitFlag = 0
class myThreadLasts(threading.Thread):
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
                action, delta = self.q.get()
                queueLock.release()
                
                COMMAND = "DROP TABLE IF EXISTS "+action+"_last"
                safeExecute(COMMAND,self.con,self.cur,workQueue)
                
                COMMAND = """
                    CREATE TABLE """+action+"""_last
                    SELECT W.date AS time, 
                           W.id AS id,
                    CASE 
                        WHEN Max(e.time) IS NOT NULL THEN Max(e.time)
                        ELSE DateDiff(e.time,W.start)
                        END AS last,
                    CASE 
                        WHEN Max(e.time) IS NOT NULL THEN 1
                        ELSE 0
                        END AS has_done    
                    ,W.churned
                    FROM WCU3 AS W
                    LEFT JOIN """+action+""" AS e
                    ON W.id = e.id
                    AND DATE(e.time) < W.date
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
workQueue = Queue.Queue(10)
threads = []
threadID = 1
exitFlag = 0
# Create new threads
for tName in threadList:
    thread = myThreadLasts(threadID, tName, workQueue)
    thread.start()
    threads.append(thread)
    threadID += 1
    
queueLock.acquire()
# Fill the queue
for action in ActionList:
    workQueue.put([action,delta])
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

#Setup for parrallel run to construct count tables
exitFlag = 0
class myThreadDeltaVars(threading.Thread):
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
                action, delta = self.q.get()
                queueLock.release()
                
                COMMAND = "DROP TABLE IF EXISTS "+action+"_variance_"+str(delta)
                safeExecute(COMMAND,self.con,self.cur,workQueue)
                
                COMMAND = """
                    CREATE TABLE """+action+"""_variance_"""+str(delta)+"""
                    SELECT 
                        W.date AS time,
                        W.id AS id,
                        CASE 
                            WHEN Variance(e.time) IS NOT NULL THEN Variance(e.time)
                            ELSE 0
                        END AS variance,
                        W.churned AS churned
                    FROM WCU3 AS W
                    LEFT JOIN """+action+""" AS e
                    ON W.id = e.id
                    AND DATE(e.time) < W.date"""
                if delta > 0:
                    COMMAND += " AND DATE(e.time) >= W.date - INTERVAL "+str(delta)+" day"
                COMMAND +="""    
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
workQueue = Queue.Queue(10)
threads = []
threadID = 1
exitFlag = 0
# Create new threads
for tName in threadList:
    thread = myThreadDeltaVars(threadID, tName, workQueue)
    thread.start()
    threads.append(thread)
    threadID += 1
    
queueLock.acquire()
# Fill the queue
for delta in [0,1,2,7,28]:
    for action in ActionList:
        workQueue.put([action,delta])
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

#Setup for parrallel run to construct count tables
exitFlag = 0
class myThreadDeltaAvgs(threading.Thread):
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
                action, delta = self.q.get()
                queueLock.release()
                
                COMMAND = "DROP TABLE IF EXISTS "+action+"_average_"+str(delta)
                safeExecute(COMMAND,self.con,self.cur,workQueue)
                
                COMMAND = """
                    CREATE TABLE """+action+"""_average_"""+str(delta)+"""
                    SELECT 
                        W.date AS time, 
                        W.id AS id,
                        CASE 
                            WHEN AVG(e.time) IS NOT NULL THEN AVG(e.time)
                            ELSE 0
                        END AS average,  
                        W.churned AS churned
                    FROM WCU3 AS W
                    LEFT JOIN """+action+""" AS e
                    ON W.id = e.id
                    AND DATE(e.time) < W.date"""
                if delta > 0:
                    COMMAND += " AND DATE(e.time) >= W.date - "+str(delta)+" day"
                COMMAND +="""    
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
workQueue = Queue.Queue(10)
threads = []
threadID = 1
exitFlag = 0
# Create new threads
for tName in threadList:
    thread = myThreadDeltaAvgs(threadID, tName, workQueue)
    thread.start()
    threads.append(thread)
    threadID += 1
    
queueLock.acquire()
# Fill the queue
for delta in [0,1,2,7,28]:
    for action in ActionList:
        workQueue.put([action,delta])
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
