from flask import Flask, render_template, request
import sqlite3 as sql
import pandas as pd

app = Flask(__name__)


@app.route('/')
def home():
    return render_template('home.html')


@app.route('/upload')
def upload_csv():
    return render_template('upload.html')


@app.route('/adddata',methods = ['POST', 'GET'])
def adddata():
   if request.method == 'POST':
       con = sql.connect("database.db")
       csv = request.files['myfile']
       file = pd.read_csv(csv)

       file.to_sql('Earthquake', con, schema=None, if_exists='replace', index=True, index_label=None, chunksize=None, dtype=None)
       con.close()
   return render_template("adddata.html", msg = "Record inserted successfully")


@app.route('/display')
def display():
    conn = sql.connect("database.db")
    c = conn.cursor()
    query = "SELECT * FROM Earthquake"
    c.execute(query)

    rows = c.fetchall()

    return render_template('display.html',info = rows)


if __name__ == '__main__':
    app.run()
