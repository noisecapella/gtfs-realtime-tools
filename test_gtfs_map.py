from gtfs_map import GtfsMap
import datetime
import argparse
def main():
        parser = argparse.ArgumentParser()
        parser.add_argument("gtfs_path")
        args = parser.parse_args()
        gtfs_path = args.gtfs_path

        gtfs_map = GtfsMap(gtfs_path, False)
        rows = []
        for row in gtfs_map.find_stop_times_for_datetime(datetime.datetime.now()):
                rows.append(row)

        rows = sorted(rows, key=lambda row: row['arrival_time'])
        for row in rows:
                print(row)

        


if __name__ == "__main__":
        main()
