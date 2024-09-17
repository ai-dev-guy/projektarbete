import pandas as pd
import logging

from datetime import datetime as dt


def cleanData(input_filename="raw_weather_data.csv", output_filename="processed_weather_data.csv"):  # Tar default filnamnsargument om inga värden anges
    logging.basicConfig(level=logging.INFO)
    log = logging.getLogger(__name__)

    try:
        df = pd.read_csv(input_filename)
        log.info(f"Successfully loaded CSV data from {input_filename}")
    except pd.errors.EmptyDataError:
        log.error(f"Error: The file {input_filename} is empty.")
        raise
    except FileNotFoundError:
        log.error(f"Error: The file {input_filename} was not found.")
        raise
    except Exception as e:
        log.error(f"An unexpected error occurred while reading the file: {str(e)}")
        raise

    start_date = dt.now() - pd.Timedelta(hours=48)
    end_date = dt.now()

    required_columns = ['location.localtime_epoch', 'forecast.temp_c', 'forecast.pressure_mb', 'forecast.humidity']
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        log.error(f"Error: Missing columns in the input data: {', '.join(missing_columns)}")
        raise ValueError(f"Missing columns: {', '.join(missing_columns)}")

    df = df[(pd.to_datetime(df['location.localtime_epoch'], unit='s') >= start_date) & (pd.to_datetime(df['location.localtime_epoch'], unit='s') <= end_date)]

    df['hour'] = pd.to_datetime(df['location.localtime_epoch'], unit='s').dt.hour
    df['month'] = pd.to_datetime(df['location.localtime_epoch'], unit='s').dt.month
    df['temp'] = df['forecast.temp_c']
    df['temp_lag_1'] = df['forecast.temp_c'].shift(1)
    df['temp_lag_3'] = df['forecast.temp_c'].shift(3)
    df['pressure'] = df['forecast.pressure_mb']
    df['humidity'] = df['forecast.humidity']
    df['temp_target'] = df['forecast.temp_c'].shift(-1).rolling(24).max()
    
    columns_to_keep = ['hour', 'month', 'temp', 'humidity', 'pressure', 'temp_lag_1', 'temp_lag_3', 'temp_target']
    df = df[columns_to_keep]

    parsed_df = df.dropna()
    log.info(f"Processed {len(parsed_df)} rows of data")
