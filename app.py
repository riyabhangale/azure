from flask import Flask, render_template, request,flash
import sqlite3 as sql
import pandas as pd
from time import time
import redis
import pickle
import random
import numpy as np

app = Flask(__name__)

r = redis.StrictRedis(host='riyacloud.redis.cache.windows.net', port=6380, db=0, password='593PO2XLFETRrSYj2sSnOzznMqlLgrKjKiaCMQEB7Fw=', ssl=True)
# result = r.ping()
# print("Ping returned : " + str(result))
# r.set("msg:hello","Hello Redis!!!")
# msg = r.get("msg:hello")
# print(msg)
# print(random.uniform(0,10))



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

       data.to_sql('quake', conn, schema=None, if_exists='replace', index=True, index_label=None, chunksize=None, dtype=None)
       end_time = time()
       c = conn.cursor()
       query = "SELECT * FROM quake"
       c.execute(query)
       rows = c.fetchall()
       conn.close()
       time_taken = (end_time - start_time)
       # flash('The Average Time taken to execute the random queries is : ' + "%.4f" % time_taken + " seconds")
   return render_template("adddata.html", msg = "Record inserted successfully",data=rows,t = time_taken)



# def displayAll():
    # conn = sql.connect("database.db")
    # c = conn.cursor()
    # query = "SELECT * FROM Earthquake"
    # c.execute(query)
    # rows = c.fetchall()
    # return render_template('display.html',data = rows, len=len(rows))
#

############POint 5 ################
@app.route('/point5',methods = ['POST', 'GET'])
def point5():
    dep1 = request.form["dep1"]
    dep2 = request.form["dep2"]
    long = request.form["long"]
    conn = sql.connect("database.db")
    c = conn.cursor()
    query = 'SELECT * FROM quake where depthError between "'+dep1+'" and "'+dep2+'" and longitude > "'+long+'"'
    c.execute(query)
    rows = c.fetchall()
    # print(rows)
    return render_template('point5.html',data = rows, len=len(rows))


########### Point 6 #################
@app.route('/point6', methods=['GET', 'POST'])
def point6():
    if request.method == 'POST':
        dep1 = float(request.form["dep11"])
        dep2 = float(request.form["dep22"])
        qno = int(request.form["qno"])
        temp = []
        time1 = []
        cache = "mycache"
        for i in range(qno):
            res = []
            ran_num = "{:.3f}".format(random.uniform(dep1, dep2))
            ran_num2 = "{:.3f}".format(random.uniform(dep1, dep2))
            start_time = time()
            query = "select * from quake where depthError between ' " + str(ran_num) + " 'and ' " + str(ran_num2) + " ' "
            conn = sql.connect("database.db")
            c = conn.cursor()
            c.execute(query)
            rows = c.fetchall()
            # rows = np.array(rows)
            # rows = rows.flatten()
            res.append(str(len(rows)))
            res.append(ran_num)
            res.append(ran_num2)

            r.set(cache + str(i), 1)
            # print (rows)
            end_time = time() - start_time
            time1.append(end_time)
            res.append(end_time)
            temp.append(res)
        # print(temp)
        # print(temp[1])
        # print(time1)
        # print(time1)
    return render_template("point6.html", data=temp, time=time1, random = res)



@app.route('/displayAll')
def displayAll():
    keyname = 'displayAll'
    if(r.exists(keyname)):
        is_Cache = 'Query with Cache'
        start_time = time()
        rows = pickle.loads(r.get(keyname))
        end_time = time()
        time_taken = (end_time - start_time)
        # print(time_taken,is_Cache)
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
        # print(time_taken, is_Cache)
        # print(len(rows))
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
    return render_template('display1000.html',data = rows, time = time_taken)


@app.route('/displayMag',methods = ['POST', 'GET'])
def displayMag():
    for i in range(2):
        mag = random.randint(0,8)
        keyname = 'mag' + str(mag)
        print(keyname)
        if(r.exists(keyname)):
            is_Cache = 'Query with Cache'
            start_time = time()
            rows = pickle.loads(r.get(keyname))
            end_time = time()
            time_taken = (end_time - start_time)
            # print(time_taken,is_Cache)
            # r.delete(keyname)
        else:
            is_Cache = 'Query without Cache'
            start_time = time()
            conn = sql.connect("database.db")

            c = conn.cursor()
            query = 'select * from Earthquake where mag > '+str(mag)+''
            c.execute(query)

            rows = c.fetchall()
            end_time = time()
            time_taken = (end_time - start_time)
        # print(time_taken, is_Cache)
        # print(len(rows))
            conn.close()
            r.set(keyname, pickle.dumps(rows))
    return render_template("displayMag.html", data=rows, time=time_taken, isCache=is_Cache)


if __name__ == '__main__':
    app.run()
