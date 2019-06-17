from flask import Flask, render_template, request,flash
import sqlite3 as sql
import pandas as pd
from time import time
import redis
import pickle

app = Flask(__name__)

r = redis.StrictRedis(host='riyacloud.redis.cache.windows.net', port=6380, db=0, password='593PO2XLFETRrSYj2sSnOzznMqlLgrKjKiaCMQEB7Fw=', ssl=True)
# result = r.ping()
# print("Ping returned : " + str(result))
# r.set("msg:hello","Hello Redis!!!")
# msg = r.get("msg:hello")
# print(msg)


@app.route('/')
def index():
    return render_template('home.html')


@app.route('/uploadCSV')
def upload_csv():
    return render_template('upload.html')


@app.route('/addAlldata',methods = ['POST', 'GET'])
def addAlldata():
   if request.method == 'POST':
       start_time = time()
       conn = sql.connect("database.db")
       csvFile = request.files['myfileCSV']
       data = pd.read_csv(csvFile)

       data.to_sql('Earthquake', conn, schema=None, if_exists='replace', index=True, index_label=None, chunksize=None, dtype=None)
       end_time = time()
       conn.close()
       time_taken = (end_time - start_time)
       # flash('The Average Time taken to execute the random queries is : ' + "%.4f" % time_taken + " seconds")
   return render_template("adddata.html", msg = "Record inserted successfully",t = time_taken)


@app.route('/displayAll')
# def displayAll():
    # conn = sql.connect("database.db")
    # c = conn.cursor()
    # query = "SELECT * FROM Earthquake"
    # c.execute(query)
    # rows = c.fetchall()
    # return render_template('display.html',data = rows, len=len(rows))
#
def display():
    keyname = 'displayAll'
    if(r.exists(keyname)):
        is_Cache = 'Query with Cache'
        start_time = time()
        rows = pickle.loads(r.get(keyname))
        end_time = time()
        time_taken = (end_time - start_time)
        print(time_taken,is_Cache)
        r.delete(keyname)
    else:
        is_Cache = 'Query without Cache'
        start_time = time()
        conn = sql.connect("database.db")

        c = conn.cursor()
        c.execute("select * from Earthquake")

        rows = c.fetchall()
        end_time = time()
        time_taken = (end_time - start_time)
        print(time_taken, is_Cache)
        print(len(rows))
        conn.close()
        r.set(keyname, pickle.dumps(rows))
    return render_template("display.html", data=rows, time=time_taken, isCache=is_Cache)


@app.route('/display1000')
def display1000():
    conn = sql.connect("database.db")
    c = conn.cursor()
    start_time = time()
    for i in range(1000):
        query = "SELECT * FROM Earthquake"
        c.execute(query)
        rows = c.fetchall()
    end_time = time()
    time_taken = (end_time - start_time)
    return render_template('display1000.html',info = rows, time = time_taken)


if __name__ == '__main__':
    app.run()
