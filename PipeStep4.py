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

#Execute parallel table construction
queueLock = threading.Lock()
workQueue = Queue.Queue(202)
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
