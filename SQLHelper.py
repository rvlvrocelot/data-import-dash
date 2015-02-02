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
	plt.savefig(destination)

def generateTrailingMonthlyLists(QueryFunction,months,date):
	monthsList = []
	countlist = []

	monthsList.append(date.strftime("%Y%m"))
	countlist.append(int(QueryFunction(date)))

	for x in range(months - 1):
		date = date - dateutil.relativedelta.relativedelta(months=1)
		monthsList.append(str(date.strftime("%Y%m")))
		countlist.append(int(QueryFunction(date)))

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

	mon, coun = generateTrailingMonthlyLists(getAssetsUpdatedThisMonth,9,date)
	generateGraph(mon,coun,'Assets updated per month','Assets updated',"./Static/asset")

def getNewFundsThisMonth(d = datetime.datetime.now()):
	cnxn = po.connect('DRIVER={SQL Server Native Client 10.0};SERVER=GLDB;DATABASE=siGlobalResearch;Trusted_Connection=yes')
	cursor = cnxn.cursor()
	d = d.replace(day = 1)
	d2 = d + dateutil.relativedelta.relativedelta(months=1)

	d = d.strftime("%m-%d-%Y")
	d2 = d2.strftime("%m-%d-%Y")

	cursor.execute("SELECT COUNT(*) count FROM dbo.siFund WHERE dCreated >= '%s' and dCreated <= '%s' " % (str(d), str(d2)))
	result = cursor.fetchall()

	for row in result:
	    return row.count

def generateNewFundsGraph(date = datetime.datetime.now()):
	mon, coun = generateTrailingMonthlyLists(getNewFundsThisMonth,9,date)
	generateGraph(mon,coun,'New funds per month','New funds',"./Static/newFunds")

def getNewFundsThisMonth(d = datetime.datetime.now()):
	cnxn = po.connect('DRIVER={SQL Server Native Client 10.0};SERVER=GLDB;DATABASE=siGlobalResearch;Trusted_Connection=yes')
	cursor = cnxn.cursor()
	d = d.replace(day = 1)
	d2 = d + dateutil.relativedelta.relativedelta(months=1)

	d = d.strftime("%m-%d-%Y")
	d2 = d2.strftime("%m-%d-%Y")

	cursor.execute("SELECT COUNT(*) count FROM dbo.siFund WHERE dCreated >= '%s' and dCreated <= '%s' " % (str(d), str(d2)))
	result = cursor.fetchall()

	for row in result:
	    return row.count

def getICRAaddedThisMonth(d = datetime.datetime.now()):
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

def generateICRAGraph(date = datetime.datetime.now()):
	mon, coun = generateTrailingMonthlyLists(getICRAaddedThisMonth,9,date)
	generateGraph(mon,coun,'ICRA funds updated per month','ICRA funds updated',"./Static/ICRA")