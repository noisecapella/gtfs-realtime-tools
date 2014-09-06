import sqlite3

from gtfs_map import Prediction, Location
import datetime
import calendar

def make_timestamp(date):
    return calendar.timegm(date.utctimetuple())
class PredictionsStore(object):

    def __init__(self):
        self._db = sqlite3.connect("./predictions.db")
        self._db.row_factory = sqlite3.Row

        self._db.execute("CREATE TABLE IF NOT EXISTS predictions (stop_id TEXT, trip_id TEXT, estimate_minutes INTEGER, created_at TIMESTAMP)")
        self._db.execute("CREATE TABLE IF NOT EXISTS locations (trip_id TEXT, lat FLOAT, lon FLOAT, stop_id TEXT, created_at TIMESTAMP)")

    def add_prediction(self, prediction, current_date):
        current_time = make_timestamp(current_date)
        self._db.execute("INSERT INTO predictions (stop_id, trip_id, estimate_minutes, created_at) VALUES (?, ?, ?, ?)", (prediction.stop_id, prediction.trip_id, prediction.estimated_minutes, current_time))

    def commit(self):
        self._db.commit()


    def add_location(self, location, current_date):
        current_time = make_timestamp(current_date)
        self._db.execute("INSERT INTO locations (trip_id, lat, lon, stop_id, created_at) VALUES(?, ?, ?, ?, ?)", (location.trip_id, location.lat, location.lon, location.stop_id, current_time))
