import pandas as pd
import logging


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

    required_columns = ['time', 'temp_c', 'pressure_mb', 'humidity']
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        log.error(f"Error: Missing columns in the input data: {', '.join(missing_columns)}")
        raise ValueError(f"Missing columns: {', '.join(missing_columns)}")

    df['hour'] = pd.to_datetime(df['time']).dt.hour
    df['month'] = pd.to_datetime(df['time']).dt.month
    df['temp'] = df['temp_c']
    df['temp_lag_1'] = df['temp_c'].shift(1)
    df['temp_lag_3'] = df['temp_c'].shift(3)
    df['pressure'] = df['pressure_mb']
    df['humidity'] = df['humidity']
    df['temp_target'] = df['temp_c'].shift(-24)
    
    columns_to_keep = ['hour', 'month', 'temp', 'humidity', 'pressure', 'temp_lag_1', 'temp_lag_3', 'temp_target']
    df = df[columns_to_keep]

    df = df[:-24]
    parsed_df = df.dropna()
    log.info(f"Processed {len(parsed_df)} rows of data")

    try:
        parsed_df.to_csv(output_filename, index=False)
        log.info(f"Saved processed data to {output_filename}")
    except Exception as e:
        log.error(f"Error saving processed data: {str(e)}")
        raise


if __name__ == "__main__":
    try:
        cleanData("raw_weather_data.csv", "processed_weather_data.csv")
    except Exception as e:
        logging.error(f"An error occurred: {str(e)}")
        raise
