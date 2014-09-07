import subprocess
import os
import argparse
import hashlib
import smtplib
import time
import gzip
import json
import sqlite3
import gtfs_realtime_pb2
import requests
import time
import re
from datetime import datetime, timedelta
from gtfs_map import Prediction, Location
import calendar

from predictions import make_timestamp

ALERTS = "http://developer.mbta.com/lib/GTRTFS/Alerts/Alerts.pb"
TRIP_UPDATES = "http://developer.mbta.com/lib/GTRTFS/Alerts/TripUpdates.pb"
VEHICLE_POSITIONS = "http://developer.mbta.com/lib/GTRTFS/Alerts/VehiclePositions.pb"

from gtfs_map import GtfsMap
from predictions import PredictionsStore
from datetime import datetime

def parse_gtfs_time(s, date):
    groups = re.match("(\d+):(\d+):(\d+)", s).groups()
    if int(groups[0]) >= 24:
        s = "%d:%d:%d" % (int(groups[0]) - 24, int(groups[1]), int(groups[2]))
        date += timedelta(1)
    return datetime.combine(date, datetime.strptime(s, "%H:%M:%S").time())

def run_downloader(gtfs_path):
    if not os.path.isfile("./temp_gtfs.db"):
        print("Initializing gtfs map...")
        reinitialize = True
    else:
        reinitialize = False

    print("Initializing GtfsMap...")
    gtfs_map = GtfsMap(gtfs_path, reinitialize)

    predictions = PredictionsStore()
    while True:
        starting_date = datetime.now()
        
        print ("Reading trip updates...")
        data = requests.get(TRIP_UPDATES).content
        trip_message = gtfs_realtime_pb2.FeedMessage()
        trip_message.ParseFromString(data)
        
        message_date = datetime.utcfromtimestamp(trip_message.header.timestamp)

        print("Getting vehicle positions...")
        data = requests.get(VEHICLE_POSITIONS).content
        vehicle_message = gtfs_realtime_pb2.FeedMessage()
        vehicle_message.ParseFromString(data)

        print("Going through trip updates...")
        used_trips = set()
        for entity in trip_message.entity:
            if entity.trip_update:
                trip_id = entity.trip_update.trip.trip_id
                for stop_time_update in entity.trip_update.stop_time_update:
                    stop_id = stop_time_update.stop_id

                    if stop_time_update.HasField("arrival") and stop_time_update.arrival.HasField("time"):
                        estimated_minutes = int((datetime.utcfromtimestamp(stop_time_update.arrival.time) - message_date).seconds / 60)
                        
                        prediction = Prediction(stop_id=stop_id, trip_id=trip_id, estimated_minutes=estimated_minutes)

                        if estimated_minutes >= -5 and estimated_minutes < 30:
                            predictions.add_prediction(prediction, message_date)
                    elif stop_time_update.HasField("arrival") and stop_time_update.arrival.HasField("delay"):
                        print("before")
                        stop_times = list(gtfs_map.find_stop_times_for_stop_trip(stop_id, trip_id))
                        print("after")
                        if len(stop_times) == 0:
                            raise Exception("Unable to find delay for stop %s trip %s" % (stop_id, trip_id))
                        elif len(stop_times) > 1:
                            raise Exception("More than one trip found for stop %s trip %s" % (stop_id, trip_id))
                        stop_times = stop_times[0]
                        arrival_date = parse_gtfs_time(stop_times['arrival_time'], message_date) + timedelta(0, stop_time_update.arrival.delay)
                        print(arrival_date)

                        estimated_minutes = int((arrival_date - message_date).seconds / 60)
                        prediction = Prediction(stop_id=stop_id, trip_id=trip_id, estimated_minutes=estimated_minutes)
                        if estimated_minutes >= -5 and estimated_minutes < 30:
                            predictions.add_prediction(prediction, message_date)


                    used_trips.add((str(stop_id), str(trip_id)))

        print("Filtering against GTFS...")
        for stop_times in gtfs_map.find_stop_times_for_datetime(message_date):
            key = (str(stop_id), str(trip_id))
            if key not in used_trips:
                arrival_date = parse_gtfs_time(stop_times['arrival_time'], message_date)

                estimated_minutes = int((arrival_date - message_date).seconds / 60)
                prediction = Prediction(stop_id=stop_id, trip_id=trip_id, estimated_minutes=estimated_minutes)
                if estimated_minutes >= -5 and estimated_minutes < 30:
                    predictions.add_prediction(prediction, message_date)
                        
        print("Writing vehicle positions to database...")
        for entity in vehicle_message.entity:
            if entity.vehicle:
                lat = entity.vehicle.position.latitude
                lon = entity.vehicle.position.longitude
                trip_id = entity.vehicle.trip.trip_id
                stop_id = entity.vehicle.stop_id

                vehicle_message_date = datetime.utcfromtimestamp(vehicle_message.header.timestamp)
                predictions.add_location(Location(trip_id=trip_id, lat=lat, lon=lon, stop_id=stop_id), vehicle_message_date)
                
        predictions.commit()

        now = datetime.now()
        diff = now - starting_date
        print ("That took %s") % diff
        if diff.seconds > 60:
            print("Not sleeping, execution longer than a minute")
        else:
            print ("Done, sleeping for the rest of the minute...")
        time.sleep(60 - diff.seconds)

def send_email(msg):
    smtpObj = smtplib.SMTP('smtp.gmail.com:587')

    with open("/home/pi/.credentials") as f:
        credentials = json.load(f)
        email = credentials['email']
        password = credentials['pass']

    smtpObj.ehlo()
    smtpObj.starttls()
    smtpObj.login(email, password)
    smtpObj.sendmail(email, [email], msg)
    smtpObj.close()

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("gtfs_path")
    args = parser.parse_args()

    if not os.path.isdir(args.gtfs_path):
        raise Exception("gtfs_path is not a directory")

    run_downloader(args.gtfs_path)
        

if __name__ == "__main__":
    main()
