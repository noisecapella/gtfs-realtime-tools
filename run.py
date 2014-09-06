import subprocess
import os
import argparse
import hashlib
import smtplib
import time
import gzip
import json

ALERTS = "http://developer.mbta.com/lib/GTRTFS/Alerts/Alerts.pb"
TRIP_UPDATES = "http://developer.mbta.com/lib/GTRTFS/Alerts/TripUpdates.pb"
VEHICLE_POSITIONS = "http://developer.mbta.com/lib/GTRTFS/Alerts/VehiclePositions.pb"


def run_downloader():
    while True:
        

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
    args = parser.parse_args()

    try:
        run_downloader()
        
    except Exception as e:
        send_email(str(e))
        print("Successfully send error email.")

if __name__ == "__main__":
    main()
