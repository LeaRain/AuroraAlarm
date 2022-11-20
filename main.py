import argparse
import time
import pandas as pd
import notify2


class AuroraHandler:
    def __init__(self, data_source="https://services.swpc.noaa.gov/json/ovation_aurora_latest.json"):
        self.data_source = data_source
        self.aurora_data = None
        self.observation_time = None
        self.forecast_time = None
        self.update_aurora_data()

    def get_current_aurora_data(self):
        raw_aurora_data = pd.read_json(self.data_source)
        return raw_aurora_data

    def get_observation_and_forecast_time(self, aurora_data):
        self.observation_time, self.forecast_time = aurora_data[["Observation Time", "Forecast Time"]].iloc[0]

    def get_aurora_coordinate_data(self, aurora_data):
        self.aurora_data = pd.DataFrame(aurora_data["coordinates"].to_list(),
                                        columns=["longitude", "latitude", "aurora"])

    def get_probability_at_coordinates(self, longitude, latitude):
        location_data = self.aurora_data.loc[(self.aurora_data["longitude"] == longitude)
                                             & (self.aurora_data["latitude"] == latitude)]["aurora"]

        probability_list = location_data.to_list()
        if not probability_list:
            return None

        return probability_list[0]

    def update_aurora_data(self):
        raw_data = self.get_current_aurora_data()
        self.get_observation_and_forecast_time(raw_data)
        self.get_aurora_coordinate_data(raw_data)

    def get_formatted_forecast_time(self):
        time = self.forecast_time.replace(":00Z", "")
        time = time.replace(time[len(time) - 2:], "00")
        return time


class WeatherHandler:
    def __init__(self, longitude, latitude):
        self.data_source = "https://api.open-meteo.com/v1/forecast?latitude={}&longitude={}&hourly=cloudcover".format(
            latitude, longitude)

        self.cloud_cover_data = None
        self.update_weather_data()

    def get_current_cloud_cover_data(self):
        raw_cloud_data = pd.read_json(self.data_source)
        return raw_cloud_data

    def get_hourly_cloud_cover_data(self, raw_data):
        hour_data = raw_data["hourly"]
        time_frame = pd.DataFrame(hour_data["time"], columns=["Forecast Time"])
        cloud_cover_frame = pd.DataFrame(hour_data["cloudcover"], columns=["Probability"])
        self.cloud_cover_data = pd.concat([time_frame, cloud_cover_frame], axis=1)

    def get_cloud_cover_at_time(self, time):
        forecast_data = self.cloud_cover_data.loc[self.cloud_cover_data["Forecast Time"] == time]["Probability"]
        probability_list = forecast_data.to_list()

        if not probability_list:
            return None

        return probability_list[0]

    def update_weather_data(self):
        raw_data = self.get_current_cloud_cover_data()
        self.get_hourly_cloud_cover_data(raw_data)


class InterfaceHandler:
    def __init__(self):
        self.args = None
        self.init_arg_parser()
        self.latitude = 0
        self.longitude = 0
        self.update_interval = 0
        self.data_source = "https://photon.komoot.io/api/?q="

    def init_arg_parser(self):
        parser = argparse.ArgumentParser(description=
                                         "Aurora Forecaster with Weather Check")
        location_coordinate_exclusive_group = parser.add_mutually_exclusive_group(required=True)
        location_coordinate_exclusive_group.add_argument("-c", "--coordinates", action="store", nargs=2, type=int)
        location_coordinate_exclusive_group.add_argument("-l", "--location", action="store", nargs=1, type=str)
        parser.add_argument("-u", "--update", nargs="?", type=int, help="Specify update interval (in minutes)")
        self.args = parser.parse_args()

    def validate_and_process_input(self):
        if not self.args.coordinates:
            self.get_coordinates_for_location(self.args.location)

        else:
            latitude = self.args.coordinates[0]
            longitude = self.args.coordinates[1]

            if -90 <= latitude <= 90:
                self.latitude = latitude

            if -180 <= longitude <= 180:
                self.longitude = longitude

        if self.args.update:
            self.update_interval = self.args.update

    def get_coordinates_for_location(self, location):
        location_api_string = "{}{}&limit=1".format(self.data_source, location)
        raw_data = pd.read_json(location_api_string)
        raw_data.features = pd.DataFrame(raw_data.features.values.tolist())["geometry"]
        raw_data.features = pd.DataFrame(raw_data.features.values.tolist())["coordinates"]
        coordinate_array = raw_data["features"].values
        coordinates_list = [coordinate_array[0][0], coordinate_array[0][1]]
        return coordinates_list

    def get_rounded_coordinates_for_location(self, location):
        coordinates = self.get_coordinates_for_location(location)
        coordinates = [round(coordinates[0]), round(coordinates[1])]
        return coordinates


class ProcessHandler:
    def __init__(self, update_interval=0):
        self.interface_handler = InterfaceHandler()
        self.aurora_handler = AuroraHandler()
        self.weather_handler = None
        self.update_interval = update_interval
        notify2.init("Aurora Notifier")
        self.notifier = notify2.Notification("Aurora Notifier")
        self.notifier.set_urgency(notify2.URGENCY_NORMAL)
        self.notifier.set_timeout(1000)

    def init_weather_handler(self):
        self.weather_handler = WeatherHandler(self.interface_handler.longitude, self.interface_handler.latitude)

    def get_current_aurora_status(self):
        return self.aurora_handler.get_probability_at_coordinates(self.interface_handler.longitude,
                                                                  self.interface_handler.latitude)

    def get_current_cloud_cover_status(self):
        return self.weather_handler.get_cloud_cover_at_time(self.aurora_handler.get_formatted_forecast_time())

    def show_current_data(self):
        aurora_status = self.get_current_aurora_status()
        cloud_cover_status = self.get_current_cloud_cover_status()
        print(aurora_status)
        print(cloud_cover_status)
        return aurora_status, cloud_cover_status

    def send_notification(self, message):
        self.notifier.update(message)
        self.notifier.show()

    def run_updates(self, update_time):
        update_time_in_seconds = update_time * 60
        while True:
            data = self.show_current_data()
            self.send_notification("Aurora: {} Cloud: {}".format(data[0], data[1]))
            time.sleep(update_time_in_seconds)

    def main(self):
        self.interface_handler.validate_and_process_input()
        self.init_weather_handler()
        if self.interface_handler.update_interval == 0:
            self.show_current_data()

        else:
            self.run_updates(self.interface_handler.update_interval)


if __name__ == '__main__':
    process_handler = ProcessHandler()
    process_handler.main()
