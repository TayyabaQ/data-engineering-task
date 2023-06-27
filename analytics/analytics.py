from os import environ
from time import sleep
from sqlalchemy import create_engine, Column, Float, String, Integer, inspect
from sqlalchemy.exc import OperationalError
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import json
from geopy.distance import geodesic
from datetime import datetime


print('Waiting for the data generator...')
sleep(20)
print('ETL Starting...')

while True:
    try:
        postgres_engine = create_engine(environ["POSTGRESQL_CS"], pool_pre_ping=True, pool_size=10)
        mysql_engine = create_engine(environ["MYSQL_CS"], pool_pre_ping=True, pool_size=10)
        break
    except OperationalError:
        sleep(0.1)
print('Connection to PostgresSQL & MYSQL successful.')

# Write the solution here
Base = declarative_base()

# Define the table structure for the PostgreSQL database
class DeviceData(Base):
    __tablename__ = 'devices'
    id = Column(Integer, primary_key=True)
    device_id = Column(String)
    temperature = Column(Integer)
    location = Column(String)
    time = Column(String)

# Define the table structure for the MySQL database
class ResultData(Base):
    __tablename__ = 'analytics_data'
    id = Column(Integer, primary_key=True)
    device_id = Column(String(50))
    hour = Column(Integer)
    max_temperature = Column(Integer)
    total_distance = Column(Float)
    data_point_count = Column(Integer)


# Create the sessions for PostgreSQL and MySQL databases
PostgresSession = sessionmaker(bind=postgres_engine)
postgres_session = PostgresSession()
MySQLSession = sessionmaker(bind=mysql_engine)
mysql_session = MySQLSession()

# Defining functions for ETL Process

def extract_data():
    # Query the PostgreSQL database and calculate total distance, max temperature, and data point count for each device and hour
    result = postgres_session.query(DeviceData.device_id, DeviceData.time, DeviceData.location, DeviceData.temperature).all()
    return result

def transform_data(result):
    grouped_data = {}
    for device_id, time, location, temperature in result:
        timestamp = int(time)
        hour = datetime.fromtimestamp(timestamp).hour

        if device_id not in grouped_data:
            grouped_data[device_id] = {}
            
        if hour not in grouped_data[device_id]:
            grouped_data[device_id][hour] = {
                'total_distance': 0.0,
                'max_temperature': float('-inf'),
                'data_point_count': 0,
                'previous_location': None
            }
            
        if grouped_data[device_id][hour]['previous_location']:
            lat1, lon1 = json.loads(grouped_data[device_id][hour]['previous_location'])['latitude'], \
                         json.loads(grouped_data[device_id][hour]['previous_location'])['longitude']
            lat2, lon2 = json.loads(location)['latitude'], json.loads(location)['longitude']
            distance = geodesic((lat1, lon1), (lat2, lon2)).miles
            grouped_data[device_id][hour]['total_distance'] += distance
        
        if temperature > grouped_data[device_id][hour]['max_temperature']:
            grouped_data[device_id][hour]['max_temperature'] = temperature
        
        grouped_data[device_id][hour]['data_point_count'] += 1
        grouped_data[device_id][hour]['previous_location'] = location
    return grouped_data

def write_data(grouped_data):
    # Check if the "result_data" table exists in the MySQL database, create it if necessary
    try:
        if not inspect(mysql_engine).has_table(ResultData.__tablename__):
            ResultData.__table__.create(mysql_engine)
    except OperationalError:
        pass
    # Write the calculated values to the MySQL database
    for device_id, hour_data in grouped_data.items():
        for hour, data in hour_data.items():
            result_data = ResultData(
                device_id=device_id,
                hour=hour,
                max_temperature=data['max_temperature'],
                total_distance=data['total_distance'],
                data_point_count=data['data_point_count']
            )
            mysql_session.add(result_data)
    # Commit the changes to the MySQL database
    mysql_session.commit()

# Main ETL Function
def ETL():
    result = extract_data()
    grouped_data = transform_data(result)
    write_data(grouped_data)

# Run the ETL
ETL()

print('ETL Performed Successfully.')

# Close the sessions
postgres_session.close()
mysql_session.close()
