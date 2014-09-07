from gtfs_map import GtfsMap
import datetime
import argparse
def main():
	parser = argparse.ArgumentParser()
	parser.add_argument("gtfs_path")
	args = parser.parse_args()
	gtfs_path = args.gtfs_path

	gtfs_map = GtfsMap(gtfs_path, False)
	for row in gtfs_map.find_stop_times_for_date(datetime.datetime.now()):
		print(row)


if __name__ == "__main__":
	main()
