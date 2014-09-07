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
    diff = date - datetime.combine(date, datetime.strptime(s, "%H:%M:%S").time())
    return int(diff.seconds / 60)

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
        print ("Reading trip updates...")
        data = requests.get(TRIP_UPDATES).content
        message = gtfs_realtime_pb2.FeedMessage()
        message.ParseFromString(data)

        current_date = datetime.now()

        print("Going through trip updates...")
        current_timestamp = make_timestamp(current_date)
        for entity in message.entity:
            if entity.trip_update:
                trip_id = entity.trip_update.trip.trip_id
                for stop_time_update in entity.trip_update.stop_time_update:
                    stop_id = stop_time_update.stop_id

                    if stop_time_update.departure and stop_time_update.departure.time is not None:
                        estimated_minutes = int((stop_time_update.departure.time - current_timestamp) / 60)
                        
                        prediction = Prediction(stop_id=stop_id, trip_id=trip_id, estimated_minutes=estimated_minutes)
                        if estimated_minutes >= 0 and prediction.estimated_minutes < 30:
                            predictions.add_prediction(prediction, current_date)
                    elif stop_time_update.departure and stop_time_update.departure.delay is not None:
                        trip = list(gtfs_map.find_stop_times_for_stop_trip(stop_id, trip_id))
                        if len(trip) == 0:
                            raise Exception("Unable to find delay for stop %s trip %s" % (stop_id, trip_id))
                        elif len(trip) > 1:
                            raise Exception("More than one trip found for stop %s trip %s" % (stop_id, trip_id))
                        departure_time = parse_gtfs_time(trip['departure_time'], current_date)
                        estimated_minutes = int((departure_time - current_timestamp) / 60)
                        prediction = Prediction(stop_id=stop_id, trip_id=trip_id, estimated_minutes=estimated_minutes)
                        if estimated_minutes >= 0 and prediction.estimated_minutes < 30:
                            predictions.add_prediction(prediction, current_date)
                        
                    else:
                        print("Unknown stop_id %s trip_id %s entity %s" % (stop_id, trip_id, entity))

        print("Getting vehicle positions...")
        data = requests.get(VEHICLE_POSITIONS).content
        message = gtfs_realtime_pb2.FeedMessage()
        message.ParseFromString(data)

        print("Writing vehicle positions to database...")
        for entity in message.entity:
            if entity.vehicle:
                lat = entity.vehicle.position.latitude
                lon = entity.vehicle.position.longitude
                trip_id = entity.vehicle.trip.trip_id
                stop_id = entity.vehicle.stop_id

                predictions.add_location(Location(trip_id=trip_id, lat=lat, lon=lon, stop_id=stop_id), current_date)
                
        predictions.commit()
        print ("Done, sleeping for a minute...")
        time.sleep(60)

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
