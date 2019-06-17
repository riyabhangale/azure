from flask import Flask, render_template, request,flash
import sqlite3 as sql
import pandas as pd
from time import time

app = Flask(__name__)


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
       csvFile = request.files['myfile']
       data = pd.read_csv(csvFile)

       data.to_sql('Earthquake', conn, schema=None, if_exists='replace', index=True, index_label=None, chunksize=None, dtype=None)
       end_time = time()
       conn.close()
       time_taken = (end_time - start_time)
       # flash('The Average Time taken to execute the random queries is : ' + "%.4f" % time_taken + " seconds")
   return render_template("adddata.html", msg = "Record inserted successfully",t = time_taken)


@app.route('/displayAll')
def displayAll():
    conn = sql.connect("database.db")
    c = conn.cursor()
    query = "SELECT * FROM Earthquake"
    c.execute(query)
    rows = c.fetchall()
    return render_template('display.html',info = rows)


if __name__ == '__main__':
    app.run()
