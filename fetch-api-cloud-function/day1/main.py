import requests
from google.cloud import storage
import base64
from flask import jsonify
import logging
import os
import datetime
 
def callapi_day1(request, context):
    logging.basicConfig(level=logging.INFO)
    log = logging.getLogger(__name__)
    try:
        #Variables for Python
        # lat= 59.3293
        # lon = 18.0686
        # apiKey = os.getenv('API_KEY')
        # apiAdress = 'https://api.openweathermap.org/'
        # url = f'{apiAdress}data/2.5/weather?lat={lat}&lon={lon}&type=hour&appid={apiKey}'

        today = datetime.date.today()
        two_days_ago = today - datetime.timedelta(days=2)
        twodaysago_date = two_days_ago.strftime("%Y-%m-%d")
        today_date= today.strftime("%Y-%m-%d")

        weatherapidotcom_key= os.getenv('Weatherapidotcom_Key')

        weatherapi = f'http://api.weatherapi.com/v1/history.json?key={weatherapidotcom_key}&q=Stockholm&dt={twodaysago_date}&end_dt={today_date}'
        response = requests.get(weatherapi)
        log.info('Python Variables set')
        #Variables For GCS
        client = storage.Client()
        storage_name = 'dataengineering-projektarbete-bucket'
        bucket = client.bucket(storage_name)
        item = bucket.blob('day1_weather.json')
        log.info('GCS Variables set')
        #Upload
        item.upload_from_string(response.content)
        log.info(f'Upload successful! Status code: {response.status_code}')
        return jsonify(response.json())
    
    except Exception as e:
        log.error(f'Upload failed! Status code: {e}')
        return None