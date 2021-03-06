import os
import csv
from datetime import datetime, timedelta
import sqlite3
from collections import namedtuple

Prediction = namedtuple('Prediction', ['stop_id', 'trip_id', 'estimated_minutes'])
Location = namedtuple('Location', ['trip_id', 'lat', 'lon', 'stop_id'])

class GtfsMap(object):
    def __init__(self, gtfs_path, reinitialize=True, skip_stop_times=False):
        self._db = sqlite3.connect("./temp_gtfs.db")
        self._db.row_factory = sqlite3.Row


        calendar_path = os.path.join(gtfs_path, "calendar.txt")
        self.last_date = None
        with open(calendar_path) as f:
            for row in csv.DictReader(f):
                date = datetime.strptime(row["end_date"], '%Y%m%d')
                if self.last_date is None or self.last_date < date:
                    self.last_date = date

        if not reinitialize:
            return

        self._drop_table("trips")
        self._create_table(gtfs_path, "trips", {"route_id" : "TEXT",
                                                "service_id" : "TEXT",
                                                "trip_id" : "TEXT PRIMARY KEY",
                                                "trip_headsign": "TEXT",
                                                "trip_short_name" : "TEXT",
                                                "direction_id" : "INTEGER",
                                                "block_id" : "TEXT",
                                                "shape_id" : "TEXT"})
        self._drop_table("stops")
        self._create_table(gtfs_path, "stops", {"stop_id": "TEXT PRIMARY KEY",
                                                "stop_code": "TEXT",
                                                "stop_name": "TEXT",
                                                "stop_desc": "TEXT",
                                                "stop_lat": "TEXT",
                                                "stop_lon": "TEXT",
                                                "zone_id": "TEXT",
                                                "stop_url": "TEXT",
                                                "location_type": "INTEGER",
                                                "parent_station": "TEXT"})
        self._drop_table("routes")
        self._create_table(gtfs_path, "routes", {"route_id": "TEXT PRIMARY KEY",
                                                 "agency_id": "TEXT",
                                                 "route_short_name": "TEXT",
                                                 "route_long_name": "TEXT",
                                                 "route_desc": "TEXT",
                                                 "route_type": "INTEGER",
                                                 "route_url": "TEXT",
                                                 "route_color": "TEXT",
                                                 "route_text_color": "TEXT"})
        self._drop_table("stop_times")
        self._create_table(gtfs_path, "stop_times", {"trip_id": "TEXT",
                                                     "arrival_time": "TEXT",
                                                     "departure_time": "TEXT",
                                                     "stop_id": "TEXT",
                                                     "stop_sequence": "INTEGER",
                                                     "stop_headsign": "TEXT",
                                                     "pickup_type": "INTEGER",
                                                     "drop_off_type": "INTEGER"})
        self._drop_table("shapes")
        self._create_table(gtfs_path, "shapes", {"shape_id": "TEXT",
                                                 "shape_pt_lat": "TEXT",
                                                 "shape_pt_lon": "TEXT",
                                                 "shape_pt_sequence": "INTEGER",
                                                 "shape_dist_traveled": "TEXT"})
        self._drop_table("calendar")
        self._create_table(gtfs_path, "calendar", {"service_id" : "TEXT",
                                                   "monday" : "INTEGER",
                                                   "tuesday" : "INTEGER",
                                                   "wednesday" : "INTEGER",
                                                   "thursday" : "INTEGER",
                                                   "friday" : "INTEGER",
                                                   "saturday" : "INTEGER",
                                                   "sunday" : "INTEGER",
                                                   "start_date" : "TEXT",
                                                   "end_date" : "TEXT"})
        self._drop_table("calendar_dates")
        self._create_table(gtfs_path, "calendar_dates", {"service_id" : "TEXT",
                                                         "date" : "TEXT",
                                                         "exception_type" : "INTEGER"})
                                                   

        self._import_table(gtfs_path, "trips")
        self._create_index("trips", "shape_id")
        self._create_index("trips", "route_id")
        self._create_index("trips", "service_id")
        self._import_table(gtfs_path, "stops")
        self._import_table(gtfs_path, "routes")
        if not skip_stop_times:
            self._import_table(gtfs_path, "stop_times")
            self._create_index("stop_times", "stop_id")
            self._create_index("stop_times", "trip_id")
            self._create_index("stop_times", "arrival_time")
        self._import_table(gtfs_path, "shapes")
        self._create_index("shapes", "shape_id")
        self._import_table(gtfs_path, "calendar")
        self._import_table(gtfs_path, "calendar_dates")
        
    def _import_table(self, gtfs_path, table):
        path = os.path.join(gtfs_path, table + ".txt")
        with open(path) as f:
            reader = csv.reader(f)
            header = next(reader)
            
            joined_keys = ",".join(("'%s'" % item) for item in header)
            joined_values = ",".join("?" for item in header)
            
            query = "INSERT INTO %s (%s) VALUES (%s)" % (table, joined_keys, joined_values)
            self._db.executemany(query, reader)

    def _drop_table(self, table):
        self._db.execute("DROP TABLE IF EXISTS %s" % table)


    def _create_table(self, gtfs_path, table, types):
        path = os.path.join(gtfs_path, table + ".txt")
        with open(path) as f:
            reader = csv.reader(f)
            columns = next(reader)
            
            for column in columns:
                if column not in types:
                    print ("Type for column not found: %s" % column)
                    type = "TEXT"
                else:
                    type = types[column]
            joined_columns = ",".join(["%s %s" % (column, type) for column in columns])
            self._db.execute("CREATE TABLE %s (%s)" % (table, joined_columns))


    def _create_index(self, table, column):
        self._db.execute("CREATE INDEX idx_%s_%s ON %s (%s)" % (table, column, table, column))
    
    def _query(self, query, parameters):
        return (dict(row) for row in self._db.execute(query, parameters))

    def find_routes_by_name(self, name):
        return self._query("SELECT * FROM routes WHERE route_long_name = ? OR route_short_name = ?", (name, name))

    def find_shapes_by_route(self, route):
        return self._query("SELECT DISTINCT shapes.* FROM shapes JOIN trips ON shapes.shape_id = trips.shape_id WHERE route_id = ?", (route,))

    def find_routes_by_route_type(self, route_type):
        return self._query("SELECT routes.* FROM routes WHERE route_type = ?", (route_type,))

    def find_stops_by_route(self, route):
        return self._query("SELECT DISTINCT stops.* FROM stops JOIN stop_times ON stop_times.stop_id = stops.stop_id JOIN trips ON stop_times.trip_id = trips.trip_id WHERE route_id = ?", (route,))

    def find_trips_by_route(self, route):
        return self._query("SELECT trips.* FROM trips WHERE route_id = ?", (route,))

    def find_stop_times_for_stop_trip(self, stop_id, trip_id, stop_sequence):
        return self._query("SELECT s_t.* FROM stop_times s_t WHERE s_t.trip_id = ? AND s_t.stop_id = ? AND s_t.stop_sequence = ?", (trip_id, stop_id, stop_sequence))

    def _stop_time_clause(self, date, after_hours):
        if after_hours:
            date = date + timedelta(-1)

        query = ''
        day_of_week = date.weekday()
        if day_of_week == 0:
            query += " monday = 1"
        elif day_of_week == 1:
            query += " tuesday = 1"
        elif day_of_week == 2:
            query += " wednesday = 1"
        elif day_of_week == 3:
            query += " thursday = 1"
        elif day_of_week == 4:
            query += " friday = 1"
        elif day_of_week == 5:
            query += " saturday = 1"
        elif day_of_week == 6:
            query += " sunday = 1"
        
        query += " AND start_date <= ? AND end_date >= ? AND arrival_time >= ? AND arrival_time < ? "

        date_string = date.strftime("%Y%m%d")
        arrival_time_minus_30 = date + timedelta(0, -30*60)
        arrival_time_plus_30 = date + timedelta(0, 30*60)

        def minutes_seconds(date):
            return date.strftime("%M:%S")
        def hours(date):
            if after_hours:
                hour = int(date.strftime("%H"))
                hour += 24
                return str(hour)
            return date.strftime("%H")

        def gtfs_time_from_date(date):
            return "%s:%s" % (hours(date), minutes_seconds(date))

        return (query, (date_string, date_string, gtfs_time_from_date(arrival_time_minus_30), gtfs_time_from_date(arrival_time_plus_30)))
                        

    def find_stop_times_for_datetime(self, date):
        query = "SELECT s_t.*, route_id FROM calendar AS c JOIN trips AS t ON c.service_id = t.service_id JOIN stop_times AS s_t ON s_t.trip_id = t.trip_id "


        # TODO: appropriate time zone handling for times
        # TODO: calendar_dates

        parameters = ()

        query += " WHERE ("
        sub_query, sub_params = self._stop_time_clause(date, False)
        query += " (" + sub_query + ") " 
        parameters += sub_params

        query += " OR "

        sub_query, sub_params = self._stop_time_clause(date, True)
        query += " (" + sub_query + ") "
        parameters += sub_params

        query += " )"

        return self._query(query, parameters)

 
    def __del__(self):
        self._db.commit()
        self._db.close()
