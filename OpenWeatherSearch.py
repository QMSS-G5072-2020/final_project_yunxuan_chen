import requests
import pandas as pd
import time
import datetime
import re


def get_search_weather(queries):
    """
    Access current weather data for any location.
    :param queries: query parameters to https://api.openweathermap.org.
    :return: wrapped historical data as type of Pandas DataFrame.
    """
    history = pd.DataFrame(columns=['temp', 'feels_like', 'pressure', 'humidity', 'dew_point', 'clouds',
                                    'visibility', 'wind_speed', 'wind_deg', 'weather_main', 'weather_description'])
    parameters = {'lat': queries['lat'], 'lon': queries['lon'], 'appid': 'f5b73a40cc226fdfee20569a602d8e7d'}
    days = queries['days']

    for day in range(1, days + 1):
        dt = int(time.mktime((datetime.datetime.now() - datetime.timedelta(days=day)).timetuple()))
        parameters['dt'] = dt
        try:
            req = requests.get(url='https://api.openweathermap.org/data/2.5/onecall/timemachine', params=parameters,)
            new_df = data_process(req.json()['hourly'])
            history = history.append(new_df)
        except:
            print(f'scratch weather data for previous {day} day(s) failed.')

    return history


def data_process(data):
    """
    wrangle data and transform them into Pandas DataFrame ready for analysis.
    :param data: input data included in a dictionary.
    :return: wrapped data as type of Pandas DataFrame.
    """
    data_dict = {}
    for hourly_data in data:
        data_dict.setdefault('time', [])
        if 'dt' in hourly_data:
            data_dict['time'].append(datetime.datetime.fromtimestamp(hourly_data['dt']))

        data_dict.setdefault('temp', [])
        if 'temp' in hourly_data:
            data_dict['temp'].append(hourly_data['temp'])

        data_dict.setdefault('feels_like', [])
        if 'feels_like' in hourly_data:
            data_dict['feels_like'].append(hourly_data['feels_like'])

        data_dict.setdefault('pressure', [])
        if 'pressure' in hourly_data:
            data_dict['pressure'].append(hourly_data['pressure'])

        data_dict.setdefault('humidity', [])
        if 'humidity' in hourly_data:
            data_dict['humidity'].append(hourly_data['humidity'])

        data_dict.setdefault('dew_point', [])
        if 'dew_point' in hourly_data:
            data_dict['dew_point'].append(hourly_data['dew_point'])

        data_dict.setdefault('clouds', [])
        if 'clouds' in hourly_data:
            data_dict['clouds'].append(hourly_data['clouds'])

        data_dict.setdefault('visibility', [])
        if 'visibility' in hourly_data:
            data_dict['visibility'].append(hourly_data['visibility'])

        data_dict.setdefault('wind_speed', [])
        if 'wind_speed' in hourly_data:
            data_dict['wind_speed'].append(hourly_data['wind_speed'])

        data_dict.setdefault('wind_deg', [])
        if 'wind_deg' in hourly_data:
            data_dict['wind_deg'].append(hourly_data['wind_deg'])

        data_dict.setdefault('weather_main', [])
        if 'weather' in hourly_data and 'main' in hourly_data['weather'][0]:
            data_dict['weather_main'].append(hourly_data['weather'][0]['main'])

        data_dict.setdefault('weather_description', [])
        if 'weather' in hourly_data and 'description' in hourly_data['weather'][0]:
            data_dict['weather_description'].append(hourly_data['weather'][0]['description'])

    new_df = pd.DataFrame(data_dict)
    new_df.index = pd.DatetimeIndex(new_df['time'])
    new_df.drop(['time'], axis=1, inplace=True)

    return new_df


def get_coordination(location):
    """
    Try to get lat, lon of the city by scrape from https://www.geonames.org
    :param location: city name, country code.
    :return: latitude and longitude of the city.
    """
    city, country = location.split(r', ')
    parameters = {'q': city, 'country': country}
    req = requests.get(url='https://www.geonames.org/search.html', params=parameters)
    text = re.search(r'<td nowrap>[NS].+[WE].+</td>', req.text).group()
    elements = text.split(r"</td>")

    degree = 999
    minute = 999
    second = 999
    lat = 0
    lon = 0
    for element in elements:
        if element:
            values = element.split(r"<td nowrap>")
            for value in values:
                if value:
                    numbers = value.split()
                    for number in numbers[1:]:
                        if '°' in number:
                            degree = int(number.split(r'°')[0])
                        elif "'" in number:
                            minute = int(number.split(r"'")[0])
                        elif "''" in number:
                            minute = int(number.split(r"''")[0])

                    if 'N' in value:
                        lat = degree + minute / 60 + second / 3600
                    elif 'S' in value:
                        lat = -(degree + minute / 60 + second / 3600)
                    if 'E' in value:
                        lon = degree + minute / 60 + second / 3600
                    elif 'W' in value:
                        lon = -(degree + minute / 60 + second / 3600)

    if lat > 90 or lat < -90:
        print('Wrong latitude.')
        exit(-1)
    if lon > 180 or lon < -180:
        print('Wrong longitude.')
        exit(-1)

    return lat, lon


def search_history():
    """
    main function proceed to search historical weather data.
    :return: null
    """

    print('Hello! Thanks for searching historical weather data. Please note we just provide historical weather data\n'
          'for the previous 5 days at most if you are a free account in openweathermap.org.')

    while True:
        print(
            '\n=======================================================================================================')
        queries = {}
        city_country = input("Please type the city name, like 'London, UK': ")
        lat, lon = get_coordination(city_country)
        queries['lat'] = lat
        queries['lon'] = lon
        timedelta = input("Please type the timedelta in the range 1 to 5: ")
        timedelta = int(timedelta)
        if timedelta > 5 or timedelta < 1:
            print('Invalid timedelta. It should be a integer being range from 1 to 5.')
            print("type 'q' to exit or 'c' to continue a new search.")
            option = input('Your option: ')
            if option == 'q':
                print('Goodbye...')
                break
            if option == 'c':
                print('Once again...')
                continue
            print('Invalid input, exit...')
            break
        queries['days'] = int(timedelta)

        file_time = datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d_%H-%M-%S')
        history = get_search_weather(queries)
        file_name = 'search in ' + file_time + '.csv'
        history.to_csv(file_name)

        print("Process completed. Data write to ", file_name)
        print("\nPlease type 'q' to exit or 'c' to continue a new search.")
        option = input('Your option: ')
        if option == 'q':
            print('Goodbye...')
            break
        if option == 'c':
            print('Once again...')
            continue
        print('Invalid input, exit...')
        break


if __name__ == '__main__':
    search_history()
