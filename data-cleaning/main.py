import pandas as pd
import json

from datetime import datetime


def cleanData(filename: str):
    try:
        with open(filename) as f:
            data = json.load(f)
    except json.JSONDecodeError:
        print(f"Error: The file {filename} is not a valid JSON file.")
        return
    except FileNotFoundError:
        print(f"Error: The file {filename} was not found.")
        return

    df = pd.json_normalize(data)

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

    filename_processed = filename.replace('.json', '.csv')
    parsed_df.to_csv(filename_processed, index=False)

    cleanData('weather.json')