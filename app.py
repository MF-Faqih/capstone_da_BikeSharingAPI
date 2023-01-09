import sqlite3
import requests
from tqdm import tqdm
from flask import Flask, request
import json 
import numpy as np
import pandas as pd

app = Flask(__name__) 

@app.route('/')
def home():
    return 'Hello World'


@app.route('/trips/average_duration/')
def avg_duration():
    conn = make_connection()
    nilai = get_avg_value(conn)
    return nilai.to_json()

def get_avg_value(conn):
    query = f"""SELECT bikeid, AVG(duration_minutes) AS average_usage 
    FROM trips 
    GROUP BY bikeid 
    ORDER BY average_usage"""
    result = pd.read_sql_query(query, conn)
    return result



@app.route('/trips/')
def route_all_trips():
    conn = make_connection()
    trips = get_all_trips(conn)
    return trips.to_json()

def get_all_trips(conn):
    syntax = f"""SELECT * FROM trips LIMIT"""
    hasil = pd.read_sql_query(syntax, conn)
    return hasil



@app.route('/trips/average_duration/<bike_id>')
def route_trip_id(bike_id):
    conn = make_connection()
    station = get_average_id(bike_id, conn)
    return station.to_json()

def get_average_id(bike_id, conn):
    query = f"""SELECT bikeid, AVG(duration_minutes) AS averages_time
    FROM trips 
    WHERE bikeid = {bike_id}"""
    result = pd.read_sql_query(query, conn)
    return result 



@app.route('/trips/<trips_id>')
def route_trips_id(trips_id):
    conn = make_connection()
    trips = get_trip_id(trips_id, conn)
    return trips.to_json()

def get_trip_id(trip_id, conn):
    syntax = f"""SELECT * FROM trips WHERE id = {trip_id}"""
    hasil = pd.read_sql_query(syntax, conn)
    return hasil



@app.route('/hasil', methods=['POST']) 
def post_task():

    input_data = request.get_json()
    specified_date = input_data['period']

    conn = make_connection()
    query = f"SELECT * FROM trips WHERE start_time LIKE '{specified_date}%'"

    selected_data = pd.read_sql_query(query, conn)
    
    result =  selected_data.groupby('start_station_id').agg({
    'bikeid' : 'count', 
    'duration_minutes' : 'mean'
    })

    final_data = result.reset_index()
    return final_data.to_json()




@app.route('/trips/add', methods=['POST']) 
def route_add_trips():
    # parse and transform incoming data into a tuple as we need 
    
    data = pd.Series(eval(request.get_json(force=True)))
    data = tuple(data.fillna('').values)

    conn = make_connection()
    result = insert_into_trips(data, conn)
    return result

def insert_into_trips(nilai, conn):
    syntax = f"""INSERT INTO trips values {nilai}"""
    try:
        conn.execute(syntax)
    except:
        return 'Error'
    conn.commit()
    return 'Ok'



@app.route('/json', methods=['POST']) 
def json_example():

    req = request.get_json(force=True) # Parse the incoming json data as Dictionary

    name = req['name']
    age = req['age']
    address = req['address']

    return (f'''Hello {name}, your age is {age}, and your address in {address}
            ''')



def make_connection():
    connection = sqlite3.connect('austin_bikeshare.db')
    return connection


if __name__ == '__main__':
    app.run(debug=True, port=5000)