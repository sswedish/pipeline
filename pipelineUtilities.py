def safeExecute(COMMAND,con,cur,que):
    with con:
        try:
            cur.execute(COMMAND)
        except:
            print "ERROR: occurred on command:"
            print COMMAND
            print "Emptying thread queue ..."
            que.queue.clear()

