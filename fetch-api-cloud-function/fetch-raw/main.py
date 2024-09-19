import requests
from google.cloud import storage
import base64
from flask import jsonify
import logging
import os
import datetime
import pandas as pd
#from io import BytesIO


def api_fetch(request, context):
    logging.basicConfig(level=logging.INFO)
    log = logging.getLogger(__name__)
    try:
        #Variables for Python
        # lat= 59.3293
        # lon = 18.0686
        # apiKey = os.getenv('API_KEY')
        # apiAdress = 'https://api.openweathermap.org/'
        # url = f'{apiAdress}data/2.5/weather?lat={lat}&lon={lon}&type=hour&appid={apiKey}'

        #Datetime Variables
        today = datetime.date.today()
        two_days_ago = today - datetime.timedelta(days=2)
        twodaysago_date = two_days_ago.strftime("%Y-%m-%d")
        today_date= today.strftime("%Y-%m-%d")
        weatherapidotcom_key= os.getenv('Weatherapidotcom_Key')
        log.info('Datetime Variables set')

        weatherapi = f'http://api.weatherapi.com/v1/history.json?key=d33fbab3242745b19d4100723242108&q=Stockholm&dt={twodaysago_date}&end_dt={today_date}'
        response = requests.get(weatherapi)
        
        #Variables For GCS
        client = storage.Client()
        storage_name = 'dataengineering-projektarbete-bucket'
        bucket = client.bucket(storage_name)
        item = bucket.blob('weather.json')
        item_csv = bucket.blob('raw_weather_data.csv')
        log.info('GCS Variables set')
        #Compile data
        #new_json = response.json()
        #df_new = pd.json_normalize(new_json)
        #stored_csv = item.download_as_bytes()
        #df_stored = pd.read_csv(BytesIO(stored_csv))
        #item_old = item.download_as_string().decode('utf-8')
        #df_old = pd.read_csv(StringIO(item_old))
        #log.info(f'Download success')
        #log.info('GCS Variables set')
        #combined_df = pd.concat([df_stored, df_new])
        #log.info(f'Data combined {combined_df}')


        #CSV-FRIENDLY
        data_list = []
        forecast_data = response.json
        forecast_days = forecast_data['forecast']['forecastday']
        
        for day in forecast_days:
            date = day['date']
            day_data = day['day']
            astro_data = day['astro']
        
            for hour in day['hour']:
                hour_data = {
                    'location_name': forecast_data['location']['name'],
                    'region': forecast_data['location']['region'],
                    'country': forecast_data['location']['country'],
                    'lat': forecast_data['location']['lat'],
                    'lon': forecast_data['location']['lon'],
                    'tz_id': forecast_data['location']['tz_id'],
                    'localtime_epoch': forecast_data['location']['localtime_epoch'],
                    'localtime': forecast_data['location']['localtime'],
                    'date': date,
                    'time': hour['time'],
                    'temp_c': hour['temp_c'],
                    'temp_f': hour['temp_f'],
                    'is_day': hour['is_day'],
                    'condition_text': hour['condition']['text'],
                    'condition_icon': hour['condition']['icon'],
                    'condition_code': hour['condition']['code'],
                    'wind_mph': hour['wind_mph'],
                    'wind_kph': hour['wind_kph'],
                    'wind_degree': hour['wind_degree'],
                    'wind_dir': hour['wind_dir'],
                    'pressure_mb': hour['pressure_mb'],
                    'pressure_in': hour['pressure_in'],
                    'precip_mm': hour['precip_mm'],
                    'precip_in': hour['precip_in'],
                    'snow_cm': hour['snow_cm'],
                    'humidity': hour['humidity'],
                    'cloud': hour['cloud'],
                    'feelslike_c': hour['feelslike_c'],
                    'feelslike_f': hour['feelslike_f'],
                    'dewpoint_c': hour['dewpoint_c'],
                    'dewpoint_f': hour['dewpoint_f'],
                    'vis_km': hour['vis_km'],
                    'vis_miles': hour['vis_miles'],
                    'gust_mph': hour['gust_mph'],
                    'gust_kph': hour['gust_kph'],
                    'uv': hour['uv'],
                    #Add daily data
                    'maxtemp_c': day_data['maxtemp_c'],
                    'mintemp_c': day_data['mintemp_c'],
                    'avgtemp_c': day_data['avgtemp_c'],
                    'totalprecip_mm': day_data['totalprecip_mm'],
                    'totalprecip_in': day_data['totalprecip_in'],
                    'totalsnow_cm': day_data['totalsnow_cm'],
                    'avgvis_km': day_data['avgvis_km'],
                    'avgvis_miles': day_data['avgvis_miles'],
                    'avghumidity': day_data['avghumidity'],
                    'daily_will_it_rain': day_data['daily_will_it_rain'],
                    'daily_chance_of_rain': day_data['daily_chance_of_rain'],
                    'daily_will_it_snow': day_data['daily_will_it_snow'],
                    'daily_chance_of_snow': day_data['daily_chance_of_snow'],
                    'uv_daily': day_data['uv'],
                    #Add astro data
                    'sunrise': astro_data['sunrise'],
                    'sunset': astro_data['sunset'],
                    'moonrise': astro_data['moonrise'],
                    'moonset': astro_data['moonset'],
                    'moon_phase': astro_data['moon_phase'],
                    'moon_illumination': astro_data['moon_illumination']
                }
                #Append hour data to the list
                data_list.append(hour_data)
        
                #Create DataFrame from the list
            final_df = pd.DataFrame(data_list)
        
        #Upload
        #item.upload_from_string(combined_df.to_csv(index=False), content_type='text/csv')
        item.upload_from_string(response.content)
        item_csv.upload_from_string(final_df.to_csv(index=False), content_type='text/csv')
        log.info(f'Upload successful! Status code: {response.status_code}')
        return jsonify(response.json())
    
    except Exception as e:
        log.error(f'Upload failed {e}! Status code: {response.status_code}')
        return None
        