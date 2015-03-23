import pyodbc as po
import sqlalchemy as sa
import datetime
import dateutil.relativedelta
import numpy as np
import matplotlib.pyplot as plt
import calendar
import time

cnxn = po.connect('DRIVER={SQL Server Native Client 10.0};SERVER=GLDB;DATABASE=siVendors;Trusted_Connection=yes')
cursor = cnxn.cursor()

#generate a bar graph

def generateGraph(x,y,title,ylable,destination,N=9):
	width = .35
	ind = np.arange(N)
	fig, ax = plt.subplots()
	ax.bar(ind, y, width, color='r')
	ax.set_ylabel(ylable)
	ax.set_title(title)
	ax.set_xticks(ind+width)
	ax.set_xticklabels( x )
	plt.xticks(rotation=70)
	plt.gcf().subplots_adjust(bottom=0.15)
	plt.savefig(destination)
	plt.clf()

#generate a stack graph

def generateStackedGraph(x,y,title,ylable,legend,destination,N=9):
    
    colorarray = ['r','b','g','y','c','m']
    
    width = .35
    ind = np.arange(N)
    fig, ax = plt.subplots()
    bottombar = [0]*len(y[0])
    barlist =[]
    
    for index, bars in enumerate(y):
        barlist.append(ax.bar(ind, bars, width, color=colorarray[index], bottom = bottombar)[0])
        bottombar = [i+bars[index] for index,i in enumerate(bottombar)]
        
    ax.set_ylabel(ylable)
    ax.set_title(title)
    ax.set_xticks(ind+width)
    ax.set_xticklabels( x )
    plt.xticks(rotation=70)
    plt.gcf().subplots_adjust(bottom=0.15)
    lgd = plt.legend(barlist,legend,bbox_to_anchor=(1.05, 1), loc=2, borderaxespad=0.)
    plt.savefig(destination, bbox_extra_artists=(lgd,), bbox_inches='tight')
    plt.clf()

#function to generate a trailing monthly list and execute a general query function on those months. Used by most of the processing

def generateTrailingMonthlyLists(QueryFunction,months,date):
    monthsList = []
    countlist = []

    monthsList.append(date.strftime("%Y%m"))
    countlist.append(QueryFunction(date))

    for x in range(months - 1):
        date = date - dateutil.relativedelta.relativedelta(months=1)
        monthsList.append(str(date.strftime("%Y%m")))
        countlist.append(QueryFunction(date))

    countlist.reverse()
    monthsList.reverse()

    return monthsList,countlist    

#ASSET PROCESSING

def getAssetsUpdatedThisMonth(date = datetime.datetime.now()):
	date = date.strftime("%Y%m")
	cursor.execute("SELECT count(*) count FROM LIG.Asset WHERE PeriodIDCreated = '%s' AND AssetValue IS NOT NULL "%date)
	result = cursor.fetchall()

	for row in result:
	    return row.count

def getLatestAssetDate():
	cursor.execute("SELECT Top 1 PeriodIDCreated FROM LIG.Asset ORDER BY PeriodIDCreated DESC ")
	result = cursor.fetchall()

	for row in result:
	    return datetime.datetime.strptime(str(row.PeriodIDCreated), "%Y%m")

def generateAssetGraph(date):

	monthsback = 12

	mon, coun = generateTrailingMonthlyLists(getAssetsUpdatedThisMonth,monthsback,date)
	generateGraph(mon,coun,'Assets updated per month','Assets updated',"./Static/asset",monthsback)

#ASSET THIS MONTH PROCESSING

def generateAssetMonthly(latestAssetDate):
	print "hey"
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
	    
	print assetsUpdatedList
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
    mon, coun = generateTrailingMonthlyLists(getNewFundsThisMonth,monthsback,date)
    coun = zip(*coun)
    generateStackedGraph(mon,coun,"New Funds per month","New funds",["ASIA","OFF","EURO","NA","AUST"],"./Static/newFunds",N=monthsback)


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
	    return row.count	

def generateICRAGraphRawDB(date = datetime.datetime.now()):
	monthsback = 12
	mon, coun = generateTrailingMonthlyLists(getICRAaddedThisMonthRawDB,monthsback,date)
	generateGraph(mon,coun,'ICRA funds staged on RawDB','ICRA funds updated',"./Static/ICRARawDB",monthsback)


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
	    return row.count	

def generateICRAGraph(date = datetime.datetime.now()):
	monthsback = 12
	mon, coun = generateTrailingMonthlyLists(getICRAaddedThisMonth,monthsback,date)
	generateGraph(mon,coun,'ICRA funds updated/new on siGlobalResearch','ICRA funds updated/new',"./Static/ICRA",monthsback)

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