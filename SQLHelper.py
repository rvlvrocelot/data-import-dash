import pyodbc as po
import sqlalchemy as sa
import datetime
import dateutil.relativedelta
import numpy as np
import matplotlib.pyplot as plt
import calendar
import time
import sqlite3
import collections
from operator import itemgetter

cnxn = po.connect('DRIVER={SQL Server Native Client 10.0};SERVER=GLDB;DATABASE=siVendors;Trusted_Connection=yes')
cursor = cnxn.cursor()

#generate a bar graph

class graph:


	colorarray = ['r','b','g','y','c','m']
	width = .35

	def __init__(self,x,y,title,ylable,destination,N=9,legend=None):
		self.x = x
		self.y = y
		self.title = title
		self.ylable = ylable
		self.legend = legend
		self.destination = destination
		self.N = N
		self.ind = np.arange(N)
		self.fig, self.ax = plt.subplots()
		self.ax.set_ylabel(ylable)
		self.ax.set_title(title)
		self.ax.set_xticks(self.ind+self.width)
		self.ax.set_xticklabels( x )
		plt.xticks(rotation=70)
		plt.gcf().subplots_adjust(bottom=0.15)

	def generateGraph(self):
		self.ax.bar(self.ind, self.y, self.width, color='r')
		plt.savefig(self.destination)
		plt.clf()		

	def generateStackedGraph(self):
		barlist = []
		bottombar = [0]*len(self.y[0])
		for index, bars in enumerate(self.y):
			barlist.append(self.ax.bar(self.ind, bars, self.width, color=self.colorarray[index], bottom = bottombar)[0])
			bottombar = [i+bars[index] for index,i in enumerate(bottombar)]

		lgd = plt.legend(barlist,self.legend,bbox_to_anchor=(1.05, 1), loc=2, borderaxespad=0.)
		plt.savefig(self.destination, bbox_extra_artists=(lgd,), bbox_inches='tight')
		plt.clf()

#function to generate a trailing monthly list and execute a general query function on those months. Used by most of the processing

def generateTrailingMonthlyLists(name,QueryFunction,months,date):
	monthsList = []
	countlist = []

	db = sqlite3.connect('C:\\Users\\amahan\\Desktop\\data-import-dash\\dataDash.db')

	for index, record in enumerate(QueryFunction(date)):
		db.execute('INSERT INTO monthlyData (name, value,date,number) VALUES (?, ?,?,?)',[name, record, str(date.strftime("%Y%m")),index])
		db.commit()

	trailingDate = date - dateutil.relativedelta.relativedelta(months= months -1) 
	results  = db.execute('''SELECT DISTINCT * FROM monthlyData WHERE name = ? AND date <= ? AND date >= ?  ORDER BY date ASC''', [name, str(date.strftime("%Y%m")), str(trailingDate.strftime("%Y%m"))] )


	# for month in range(20):
	# 	trailingDate = date - dateutil.relativedelta.relativedelta(months=month)
	# 	for index, record in enumerate(QueryFunction(trailingDate)):
	# 		db.execute('INSERT INTO monthlyData (name, value,date,number) VALUES (?, ?,?,?)',[name, record, str(trailingDate.strftime("%Y%m")),index])
	# 		db.commit() 

	resultsDict = collections.OrderedDict()

	for row in results: 
		print row
		if row[2] not in resultsDict:
			resultsDict[row[2]] = []
		resultsDict[row[2]].append([row[3],row[1]])

	for key in resultsDict:
		monthsList.append(key)
		resultsSet = sorted(resultsDict[key], key=itemgetter(0))
		resultList = []
		for count in resultsSet:
			resultList.append(count[1])
		countlist.append(resultList)

	print countlist,name
	return monthsList,countlist    

#ASSET PROCESSING

def getAssetsUpdatedThisMonth(date = datetime.datetime.now()):
	date = date.strftime("%Y%m")
	cursor.execute("SELECT count(*) count FROM LIG.Asset WHERE PeriodIDCreated = '%s' AND AssetValue IS NOT NULL "%date)
	result = cursor.fetchall()

	for row in result:
	    return [row.count]

def getLatestAssetDate():
	cursor.execute("SELECT Top 1 PeriodIDCreated FROM LIG.Asset ORDER BY PeriodIDCreated DESC ")
	result = cursor.fetchall()

	for row in result:
	    return datetime.datetime.strptime(str(row.PeriodIDCreated), "%Y%m")

def generateAssetGraph(date):

	monthsback = 12
	mon, coun = generateTrailingMonthlyLists("Asset",getAssetsUpdatedThisMonth,monthsback,date)
	coun = [x[0] for x in coun]
	assetGraph = graph(mon,coun,'Assets updated per month','Assets updated',"./Static/asset",monthsback)
	assetGraph.generateGraph()

#ASSET THIS MONTH PROCESSING

def generateAssetMonthly(latestAssetDate):
	cnxn = po.connect('DRIVER={SQL Server Native Client 10.0};SERVER=GLDB;DATABASE=siVendors;Trusted_Connection=yes')
	cursor = cnxn.cursor()

	assetsUpdatedList = []

	year =  latestAssetDate.year
	month = latestAssetDate.month
	periodid = latestAssetDate.strftime("%Y%m")
	print periodid
	dayrange = calendar.monthrange(year,month)[1]
	
	for y in range(1,dayrange):
	    cursor.execute('''

	        SELECT COUNT(*) count FROM siVendors.LIG.Asset
	        WHERE PeriodIdCreated = '%s'
	        AND AssetValue IS NOT NULL
	        AND dCreated > '%s-%s-%s'
	        AND dCreated < '%s-%s-%s'

	    '''%(periodid,year,month+1,y-1,year,month+1,y))
	    result = cursor.fetchall()
	        
	    for row in result:
	        assetsUpdatedList.append(row.count)
	    
	xaxis = range(len(assetsUpdatedList))
	fig, ax = plt.subplots()
	ax.set_ylabel("Assets updated")
	ax.set_xlabel("Day")
	ax.set_title("Assets updated by day this month")
	plt.plot(xaxis, assetsUpdatedList, '-o')
	plt.savefig("./Static/assetMonth")


#NEW FUND PROCESSING

def getNewFundsThisMonth(d = datetime.datetime.now()):
    cnxn = po.connect('DRIVER={SQL Server Native Client 10.0};SERVER=GLDB;DATABASE=siGlobalResearch;Trusted_Connection=yes')
    cursor = cnxn.cursor()
    d = d.replace(day = 1)
    d2 = d + dateutil.relativedelta.relativedelta(months=1)

    d = d.strftime("%m-%d-%Y")
    d2 = d2.strftime("%m-%d-%Y")

    regionList = ["ASIA","OFF","EURO","NA","AUST"]
    
    returnList = []
    
    for region in regionList:
    
        cursor.execute('''
        
        SELECT COUNT(*) count
        FROM dbo.siFund f
        JOIN  dbo.siPortfolio p ON f.PortfolioId = p.PortfolioId
        JOIN dbo.siDomicile d ON p.DomicileId = d.DomicileId
        WHERE f.dCreated >= '%s' and f.dCreated <= '%s' AND regionID = '%s' 
        
        '''% (str(d), str(d2), region))
        result = cursor.fetchall()
        
        for row in result:
             returnList.append(row.count)
                
    return returnList

def generateNewFundsGraph(date = datetime.datetime.now()):
    monthsback = 12
    mon, coun = generateTrailingMonthlyLists("funds",getNewFundsThisMonth,monthsback,date)
    coun = zip(*coun)

    newFundsGraph = graph(mon,coun,"New Funds per month","New funds","./Static/newFunds",N=monthsback,legend=["ASIA","OFF","EURO","NA","AUST"])
    newFundsGraph.generateStackedGraph()


#ICRA processing

def getICRAaddedThisMonthRawDB(d = datetime.datetime.now()):
	cnxn = po.connect('DRIVER={SQL Server Native Client 10.0};SERVER=GLDB;DATABASE=RawDB;Trusted_Connection=yes')
	cursor = cnxn.cursor()
	d = d.replace(day = 1)
	d2 = d + dateutil.relativedelta.relativedelta(months=1)

	d = d.strftime("%m-%d-%Y")
	d2 = d2.strftime("%m-%d-%Y")

	cursor.execute("SELECT COUNT(*) count FROM ICRA.Asset WHERE AssetDate >= '%s' and AssetDate <= '%s' " % (str(d), str(d2)))
	result = cursor.fetchall()

	for row in result:
		return [row.count]

def generateICRAGraphRawDB(date = datetime.datetime.now()):
	monthsback = 12
	mon, coun = generateTrailingMonthlyLists("ICRARawDB",getICRAaddedThisMonthRawDB,monthsback,date)
	coun = [x[0] for x in coun]
	print coun,mon
	ICRARawDB = graph(mon,coun,'ICRA funds staged on RawDB','ICRA funds updated',"./Static/ICRARawDB",monthsback)
	ICRARawDB.generateGraph()
	#generateGraph(mon,coun,'ICRA funds staged on RawDB','ICRA funds updated',"./Static/ICRARawDB",monthsback)


def getICRAaddedThisMonth(d = datetime.datetime.now()):
	cnxn = po.connect('DRIVER={SQL Server Native Client 10.0};SERVER=GLDB;DATABASE=siGlobalResearch;Trusted_Connection=yes')
	cursor = cnxn.cursor()
	d = d.replace(day = 1)
	d2 = d + dateutil.relativedelta.relativedelta(months=1)

	d = d.strftime("%m-%d-%Y")
	d2 = d2.strftime("%m-%d-%Y")

	cursor.execute("SELECT COUNT(*) count FROM dbo.siAsset WHERE dModified >= '%s' and dModified <= '%s' and (cModified  = 'India Rollup Asset' or cModified = 'India New Launches' ) " % (str(d), str(d2)))
	result = cursor.fetchall()

	for row in result:
	    return [row.count]	

def generateICRAGraph(date = datetime.datetime.now()):
	monthsback = 12
	mon, coun = generateTrailingMonthlyLists("ICRA",getICRAaddedThisMonth,monthsback,date)
	coun = [x[0] for x in coun]
	ICRAGraph = graph(mon,coun,'ICRA funds updated/new on siGlobalResearch','ICRA funds updated/new',"./Static/ICRA",monthsback)
	ICRAGraph.generateGraph()
	#generateGraph(mon,coun,'ICRA funds updated/new on siGlobalResearch','ICRA funds updated/new',"./Static/ICRA",monthsback)

#CBSO processing

def getFundManagersLeft():
	cnxn = po.connect('DRIVER={SQL Server Native Client 10.0};SERVER=GLDB;Trusted_Connection=yes')
	cursor = cnxn.cursor()
	cursor.execute('''

						SELECT a.ParticipantName
  							FROM (
								SELECT DISTINCT ParticipantName 
	  								FROM Vendors.CBSO.OriginationMaster
  							) a
							LEFT JOIN (
	  							SELECT DISTINCT ParticipantName
	    						FROM Vendors.CBSO.OriginationMaster
	    						WHERE PeriodId = (
	  	  							SELECT PeriodID
	  	    						FROM siGLobalResearch.dbo.siPeriod
	  	    						WHERE CurrentPeriod = 1
	    					)
						) b ON a.ParticipantName = b.ParticipantName
  					WHERE b.ParticipantName IS NULL

	''')
	result = cursor.fetchall()
	return result

	#IE processing

def getDuplicateFundCodes():
	cnxn = po.connect('DRIVER={SQL Server Native Client 10.0};SERVER=GLDB;Trusted_Connection=yes')
	cursor = cnxn.cursor()
	cursor.execute('''

		SELECT Id id, fundId, FUND_CODE fundCode, IFIC_CODE ificCode, SOURCE source FROM [InvestorEconomics].[dbo].[FundCodeMap] 
		WHERE FUND_CODE IN (SELECT FUND_CODE
		FROM [InvestorEconomics].[dbo].[FundCodeMap]
		WHERE FUND_CODE IS NOT null
		GROUP BY FUND_CODE
		HAVING COUNT(DISTINCT FundId) >1 )
		ORDER BY FUND_CODE

	''')
	results = cursor.fetchall()
	fundCodes = []
	for row in results:
		fundCodes.append([row.id, row.fundId, row.fundCode, row.ificCode])
	return fundCodes