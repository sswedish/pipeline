import datetime
#create names for distinct threads to be used by program
#number specified by user in first command line argument#
threadList = []        
#for i in range(sys.argv[1]):
for i in range(16):
    threadList += ["Thread-"+str(i)]
ActionList = ["EDUse","vPrice","LandAn","Ed20","Ed50",
               "ClickPub","CustFav","CustDom","CustSubDom",
               "CustFBIm","SEO"]
start = datetime.date(2014,1,1)
end = datetime.date(2014,7,20)

