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

from pipelineConfig import ActionList, threadList 
from pipelineUtilities import safeExecute 
#Setup for parrallel run to construct count tables
exitFlag = 0


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
                action = self.q.get()[0]
                queueLock.release()
                
                COMMAND = "DROP TABLE IF EXISTS "+action+"_last"
                safeExecute(COMMAND,self.con,self.cur,workQueue)
                
                COMMAND = """
                    CREATE TABLE """+action+"""_last
                    SELECT W.date AS time, 
                           W.id AS id,
                    CASE 
                        WHEN Max(e.time) IS NOT NULL THEN Max(e.time)
                        ELSE DateDiff(e.time,W.minTime)
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

#Execute parallel table construction
queueLock = threading.Lock()
workQueue = Queue.Queue(202)
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
    workQueue.put([action])
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
