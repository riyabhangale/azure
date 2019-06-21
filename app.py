from flask import Flask, render_template, request,flash, jsonify
import sqlite3 as sql
import pandas as pd
from time import time
import redis
import pickle
import random
import numpy as np

app = Flask(__name__)

r = redis.StrictRedis(host='riyacloud.redis.cache.windows.net', port=6380, db=0, password='593PO2XLFETRrSYj2sSnOzznMqlLgrKjKiaCMQEB7Fw=', ssl=True)


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

       data.to_sql('voting', conn, schema=None, if_exists='replace', index=True, chunksize=None, index_label=None, dtype=None)
       end_time = time()
       c = conn.cursor()
       query = "SELECT * FROM voting"
       c.execute(query)
       rows = c.fetchall()
       conn.close()
       time_taken = (end_time - start_time)
       # flash('The Average Time taken to execute the random queries is : ' + "%.4f" % time_taken + " seconds")
   return render_template("adddata.html", msg = "Record inserted successfully",data=rows,t = time_taken)


@app.route('/vote1', methods=['GET', 'POST'])
def vote1():
    conn = sql.connect("database.db")
    c = conn.cursor()
    query= "SELECT StateName as S FROM voting where TotalPop between 2000 and 8000 "
    print(query)
    rows = c.execute(query).fetchall()
    print(rows)
    print(len(rows))
    query1 = "SELECT StateName as S FROM voting where TotalPop between 8000 and 40000 "
    rows1 = c.execute(query1).fetchall()
    print(query1)
    print(rows1)
    print(len(rows1))
    return render_template('display.html',data=rows, data1 = rows1)


@app.route('/popRange1',methods = ['POST', 'GET'])
def popRange1():
    if request.method == 'POST':
        input6 = str(int(request.form['r1'])*1000)
        input7 = str(int(request.form['r2'])*1000)
        conn = sql.connect("database.db")
        c = conn.cursor()
        start_time = time()
        query = "select StateName from voting where TotalPop between '" + input6 + "' and '" + input7 + "' "
        r = c.execute(query).fetchall()
        temp = []
        for i in range(len(r)):
            dict1 = {}
            dict1['TotalPop'] = input6+' - '+input7
            dict1['StateName'] = r[i][0]
            temp.append(dict1)
        print(temp)
    # end_time = time()
    # time_taken = (end_time - start_time)
    return render_template('edu.html',data=temp)


@app.route('/popRange',methods = ['POST', 'GET'])
def popRange():
    if request.method == 'POST':
        input5 = 'col' + request.form['year']
        # year = 'col' + input5
        # print(year)
        input6 = request.form['r1']
        input7 = request.form['r2']
        input8 = request.form['r3']
        input9 = request.form['r4']
        input10 = request.form['r5']
        input11 = request.form['r6']
        conn = sql.connect("database.db")
        c = conn.cursor()
        start_time = time()
        query = "select count(State) as g1 from pop where " + input5 + " between '" + input6 + "' and '" + input7 + "' "
        query1 = "select count(State) as g2 from pop where " + input5 + " between '" + input8 + "' and '" + input9 + "'"
        query2 = "select count(State) as g3 from pop where " + input5 + " between '" + input10 + "' and '" + input11 + "'"
        # print(query)
        r = c.execute(query).fetchall()
        r1 = c.execute(query1).fetchall()
        r2 = c.execute(query2).fetchall()
        temp = []
        # for i in range(len(rows)):
        dict1 = {}
        dict1['range'] = input6+' - '+input7
        dict1['state'] = r[0][0]
        temp.append(dict1)
        dict1 = {}
        dict1['range'] = input8 + ' - ' + input9
        dict1['state'] = r1[0][0]
        temp.append(dict1)
        dict1 = {}
        dict1['range'] = input10 + ' - ' + input11
        dict1['state'] = r2[0][0]
        temp.append(dict1)
        print(temp)
    end_time = time()
    time_taken = (end_time - start_time)
    return render_template('popRange.html', t=time_taken, rec=r[0][0], rec1=r1[0][0], rec2=r2[0][0],data=temp)


@app.route('/edu', methods=['GET', 'POST'])
def edu():
    if request.method == 'POST':
        input3 = request.form['loc']
        input4 = request.form['y1']
        input5 = request.form['y2']

        conn = sql.connect("database.db")
        c = conn.cursor()
        start_time = time()
        query= "SELECT year,blpercent FROM edu where year between '"+input4+"' and '"+input5+"' and Code = '"+input3+"'"
        # print(query)
        rows = c.execute(query).fetchall()
        # print(r)
        temp = []
        for i in range(len(rows)):
            dict1 = {}
            dict1['year'] = rows[i][0]
            dict1['blpercent'] = rows[i][1]
            temp.append(dict1)


    end_time = time()
    time_taken = (end_time - start_time)
    return render_template('edu.html', t=time_taken, data=temp)



@app.route('/popRangeInc',methods = ['POST', 'GET'])
def popRangeInc():
    if request.method == 'POST':
        # input5 = 'col' + request.form['year1']
        input6 = request.form['r11']
        in1 = int(input6)*1000
        conn = sql.connect("database.db")
        c = conn.cursor()
        # start_time = time()
        temp = []
        for i in np.arange(0, 30000, in1):
            query = "select count(StateName) as g1 from voting where TotalPop between "+str(i)+" and "+str(i + in1)+" "
            r = c.execute(query).fetchall()
            # print(query)
            dict1 = {}
            dict1['range'] = str(i/1000)+' - '+str((i + in1)/1000)
            dict1['stateCount'] = r[0][0]
            temp.append(dict1)

    return render_template('popRangeInc.html',data=temp)


############POint 5 ################
@app.route('/point5',methods = ['POST', 'GET'])
def point5():
    dep1 = request.form["dep1"]
    dep2 = request.form["dep2"]
    # long = request.form["long"]
    conn = sql.connect("database.db")
    c = conn.cursor()
    query = 'SELECT mag,depth FROM quake where depthError between "'+dep1+'" and "'+dep2+'" group by mag '
    c.execute(query)
    # c.execute('PRAGMA TABLE_INFO(quake)')
    rows = c.fetchall()
    temp = []
    for i in range(len(rows)):
        dict1 = {}
        dict1['mag'] = rows[i][0]
        dict1['depth'] = rows[i][1]
        temp.append(dict1)
    return render_template('point5.html',data =temp)


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
            res.append(str(len(rows)))
            res.append(ran_num)
            res.append(ran_num2)

            r.set(cache + str(i), 1)
            end_time = time() - start_time
            time1.append(end_time)
            res.append(end_time)
            temp.append(res)
    return render_template("point6.html", data=temp, time=time1, random = res)


##############POint 7 #######################
@app.route('/point7', methods=['GET', 'POST'])
def point7():
    if request.method == 'POST':
        dep1 = float(request.form["dep111"])
        dep2 = float(request.form["dep222"])
        qno1 = int(request.form["qno1"])
        temp = []
        cache = "mycache"
        full_start = time()
        for i in range(qno1):
            if r.exists(cache + str(i)):
                # start_t = time.time()
                rows = pickle.loads(r.get(cache + str(i)))
                temp.append(rows)
                # end_t = time.time() - start_t
                # time2.append(end_t)

            else:
                res = []
                ran_num = "{:.3f}".format(random.uniform(dep1, dep2))
                ran_num2 = "{:.3f}".format(random.uniform(dep1, dep2))
                # st = time.time()
                query = "select * from quake where depthError between ' " + str(ran_num) + " 'and ' " + str(ran_num2) + " ' "
                con = sql.connect("database.db")
                cur = con.cursor()
                cur.execute(query)
                rows = cur.fetchall()
                res.append(str(len(rows)))
                res.append(ran_num)
                res.append(ran_num2)
                temp.append(res)
                r.set(cache + str(i), pickle.dumps(res))
                # et = time.time() - st
                # time1.append(et)

        end_time = time() - full_start
    return render_template("point7.html", data=temp, time2=end_time)


############# point 8 #################
@app.route('/point8', methods=['GET', 'POST'])
def point8():
    if request.method == 'POST':
        dep1 = float(request.form["dep1111"])
        dep2 = float(request.form["dep2222"])
        qno1 = int(request.form["qno11"])
        temp = []
        time1 = []
        # time2 = []
        cache = "mycache"
        # r.delete(cache + str(0))
        for i in range(qno1):
            if r.exists(cache + str(i)):
                keyword = cache + str(i)
                print(keyword)
                start_t = time()
                rows = pickle.loads(r.get(keyword))
                temp.append(rows)
                end_t = time() - start_t
                time1.append(end_t)

            else:
                res = []
                ran_num = "{:.3f}".format(random.uniform(dep1, dep2))
                ran_num2 = "{:.3f}".format(random.uniform(dep1, dep2))
                start_time = time()
                query = "select * from quake where depthError between ' " + str(ran_num) + " 'and ' " + str(ran_num2) + " ' "
                con = sql.connect("database.db")
                cur = con.cursor()
                cur.execute(query)
                rows = cur.fetchall()
                res.append(len(rows))
                res.append(ran_num)
                res.append(ran_num2)
                temp.append(res)
                r.set(cache + str(i), pickle.dumps(res))
                end_time = time() - start_time
                time1.append(end_time)

        return render_template("point8.html", data=temp, time2=time1)


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
