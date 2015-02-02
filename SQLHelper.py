import pyodbc as po
import sqlalchemy as sa
import datetime
import dateutil.relativedelta
import numpy as np
import matplotlib.pyplot as plt
import calendar



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



def getAssetsUpdatedThisMonth(date):

	cursor.execute("SELECT count(*) count FROM LIG.Asset WHERE PeriodIDCreated = '%s' "%date)
	result = cursor.fetchall()

	for row in result:
	    return row.count

def getLatestAssetDate():
	cursor.execute("SELECT Top 1 PeriodIDCreated FROM LIG.Asset ORDER BY PeriodIDCreated DESC ")
	result = cursor.fetchall()

	for row in result:
	    return row.PeriodIDCreated


def generateAssetGraph(date):


	d = datetime.datetime.strptime(str(date), "%Y%m")
	monthsList = []
	countlist = []

	monthsList.append(d.strftime("%Y%m"))
	countlist.append(getAssetsUpdatedThisMonth(d.strftime("%Y%m")))

	for x in range(8):
		d = d - dateutil.relativedelta.relativedelta(months=1)
		monthsList.append(str(d.strftime("%Y%m")))
		countlist.append(getAssetsUpdatedThisMonth(d.strftime("%Y%m")))

	countlist.reverse()
	monthsList.reverse()

	generateGraph(monthsList,countlist,'Assets updated per month','Assets updated',"./Static/asset")

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

	d = date
	monthsList = []
	countlist = []

	monthsList.append(d.strftime("%Y%m"))
	countlist.append(getNewFundsThisMonth())

	for x in range(8):
		d = d - dateutil.relativedelta.relativedelta(months=1)
		monthsList.append(str(d.strftime("%Y%m")))
		countlist.append(getNewFundsThisMonth(d))

	countlist.reverse()
	monthsList.reverse()

	generateGraph(monthsList,countlist,'New funds per month','New funds',"./Static/newFunds")