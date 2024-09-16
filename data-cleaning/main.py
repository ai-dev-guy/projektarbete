import pandas as pd
import json
from datetime import datetime as dt
from google.cloud import storage
import base64
from flask import jsonify
import logging
 
def cleaning(request, context):
    logging.basicConfig(level=logging.INFO)
    log = logging.getLogger(__name__)
    #Variables For GCS
    try:
        client = storage.Client()
        storage_name = 'dataengineering-projektarbete-bucket'
        bucket = client.bucket(storage_name)
        item_old = bucket.blob('weather.json')
        log.info('GCS Variables set')
        #File validation
        item_new = item_old.download_as_text()
        log.info(f'Download success {item_new}')
        """    try:
            with open(item_new) as f:
                data = json.load(f)
        except json.JSONDecodeError:
            print(f"Error: The file {item_new} is not a valid JSON file.")
            return None
        except FileNotFoundError:
            print(f"Error: The file {item_new} was not found.")
            return None """

        df = pd.json_normalize(item_new)

        df['hour'] = pd.to_datetime(df['dt'],unit='s').dt.hour
        df['month'] = pd.to_datetime(df['dt'],unit='s').dt.month

        df['main.temp'] = df['main.temp'] - 273.15

        df['temp_target'] = df['main.temp'].shift(-1).rolling(24).max()
        df['temp_lag_1'] = df['main.temp'].shift(1)
        df['temp_lag_3'] = df['main.temp'].shift(3)

        parsed_df = df[
            [
                'hour', 'month', 'temp','humidity','pressure','temp_lag_1','temp_lag_3','temp_target'
            ]
        ]
        parsed_df = parsed_df.dropna()

        #filename_processed = filename.replace('.json', '.csv')
        item_new = parsed_df.to_csv('weather.csv', index=False)

        
        #Upload
        item_new.upload_from_string(parsed_df)
        log.info('Upload successful!')
        return item_new
        
    except Exception as e:
            log.error(f'Upload failed! Status code: {e}')
            return None
