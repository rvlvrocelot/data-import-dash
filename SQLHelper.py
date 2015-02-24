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
    plt.legend(barlist,legend)
    plt.savefig(destination)


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


def getAssetsUpdatedThisMonth(date = datetime.datetime.now()):
	date = date.strftime("%Y%m")
	cursor.execute("SELECT count(*) count FROM LIG.Asset WHERE PeriodIDCreated = '%s' "%date)
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