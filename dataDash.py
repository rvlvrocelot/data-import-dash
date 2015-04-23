# all the imports
import sqlite3
import json
from flask import Flask, request, session, g, redirect, url_for, \
     abort, render_template, flash
from contextlib import closing
from flask_bootstrap import Bootstrap
import hashlib
import SQLHelper
import os
import random 

# configuration
# the database is not in tmp on the deployed verson
DATABASE = 'C:\\Users\\amahan\\Desktop\\data-import-dash\\dataDash.db'
DEBUG = True
SECRET_KEY = 'development key'
USERNAME = 'admin'
PASSWORD = 'default'

# create our little application :)
app = Flask(__name__)
app.config.from_object(__name__)
Bootstrap(app)

def connect_db():
    return sqlite3.connect(app.config['DATABASE'])

def init_db():
    with closing(connect_db()) as db:
        with app.open_resource('schema.sql', mode='r') as f:
            db.cursor().executescript(f.read())
        db.commit()

def runScript(scriptName):
    with closing(connect_db()) as db:
        with app.open_resource('scripts/' + scriptName, mode='r') as f:
            db.cursor().executescript(f.read()) 
        db.commit()

@app.before_request
def before_request():
    g.db = connect_db()


@app.teardown_request
def teardown_request(exception):
    db = getattr(g, 'db', None)
    if db is not None:
        db.close()

#Load the homepage where the announcements are displayed
@app.route('/')
def ODS():
    session['refresh'] = False
    latestAssetDate = SQLHelper.getLatestAssetDate()
    AssetsUpdatedThisMonth = SQLHelper.getAssetsUpdatedThisMonth(latestAssetDate)
    NewFundsThisMonth = SQLHelper.getNewFundsThisMonth()   
    NewFundsThisMonth = sum(NewFundsThisMonth)
    SQLHelper.generateAssetGraph(latestAssetDate)
    SQLHelper.generateNewFundsGraph()
    #SQLHelper.generateAssetMonthly(latestAssetDate)

    f = open("previous.txt","r")
    previous = f.readline()
    f.close()

    fundsSinceRefresh = NewFundsThisMonth - int(previous)

    f = open("previous.txt","w")
    f.write(str(NewFundsThisMonth))
    f.close()

    return render_template('ODS.html',latestAssetDate= latestAssetDate, AssetsUpdatedThisMonth = AssetsUpdatedThisMonth, NewFundsThisMonth = NewFundsThisMonth, fundsSinceRefresh = fundsSinceRefresh)

@app.route('/ICRA')
def ICRA():
    SQLHelper.generateICRAGraphRawDB()
    SQLHelper.generateICRAGraph()
    return render_template('ICRA.html')

@app.route('/CBSO')
def CBSO():
    if session['refresh'] == False:
        refresh = True
        session['refresh'] = True   
    else:
        refresh = False 

    refresh = False

    managersLeft = SQLHelper.getFundManagersLeft()
    markets = SQLHelper.getMarkets()
    managers = SQLHelper.getManagers()
    hack = random.randint(0,5000)


    return render_template('CBSO.html', managersLeft=managersLeft, markets = markets, managers= managers, refresh = refresh, hack = hack)

@app.route('/generateCBSOGraph', methods=['POST'])
def CBSOGraph():

    session['refresh'] = False
    session['state'] = request.form["state"]
    session['manager'] = request.form["managerName"]
    session['market'] = request.form["marketName"]
    session['variables'] = request.form.getlist("variables")

    print request.form["variables"]

    if request.form["state"] == "manager":
        SQLHelper.generateCBSOGraph("ParticipantName",request.form["managerName"],request.form.getlist('variables'))

    if request.form["state"] == "market":
        SQLHelper.generateCBSOGraph("OriginationMarket",request.form["marketName"],request.form.getlist('variables'))

    return redirect("CBSO")


@app.route('/IE')
def IE():
    fundCodes = SQLHelper.getDuplicateFundCodes()
    return render_template('IE.html',fundCodes=fundCodes)
   
if __name__ == '__main__':
    app.run(host='0.0.0.0')
s
@app.route('/hindsight')
def Hindsight():
    pass
