import pyodbc as po
import sqlalchemy as sa
import datetime
import dateutil.relativedelta
import numpy as np
import matplotlib.pyplot as plt



cnxn = po.connect('DRIVER={SQL Server Native Client 10.0};SERVER=GLDB;DATABASE=siVendors;Trusted_Connection=yes')
cursor = cnxn.cursor()


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

	for x in range(5):
		d = d - dateutil.relativedelta.relativedelta(months=1)
		monthsList.append(str(d.strftime("%Y%m")))
		countlist.append(getAssetsUpdatedThisMonth(d.strftime("%Y%m")))

	countlist.reverse()
	monthsList.reverse()

	width = .35
	ind = np.arange(6)
	fig, ax = plt.subplots()
	ax.bar(ind, countlist, width, color='r')
	ax.set_ylabel('Assets updated')
	ax.set_title('Assets updated per month')
	ax.set_xticks(ind+width)
	ax.set_xticklabels( monthsList )
	plt.savefig("./Static/asset")