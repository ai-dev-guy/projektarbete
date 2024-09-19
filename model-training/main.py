from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_absolute_error, root_mean_squared_error
import joblib
import pandas as pd
import logging
from google.cloud import storage
from io import BytesIO

def trainModel() -> dict:
    logging.basicConfig(level=logging.INFO)
    log = logging.getLogger(__name__)
    model_name: str
    csv_file: str
    try:
        #Variables For GCS
        client = storage.Client()
        storage_name = 'dataengineering-projektarbete-bucket'
        bucket = client.bucket(storage_name)
        item_model = bucket.blob('weather_forecasting_model_stockholm_xgb.pkl')
        item_processed = bucket.blob('processed_weather_data.csv')
        csv_data = item_model.download_as_bytes()
        csv_file = BytesIO(csv_data)
        if not model_name.endswith('.pkl'):
            raise ValueError('The model filename must end with .pkl')
        try:
            model = joblib.load(model_name)
        except FileNotFoundError:
            log.error(f"Model file {model_name} not found. Please ensure the model exists or provide a valid model name.")
            return None

        try:
            df = pd.read_csv((csv_file))
        except pd.errors.EmptyDataError:
            raise ValueError(f'The file {csv_file} is empty or not a valid CSV file.')
        except FileNotFoundError:
            raise FileNotFoundError(f'The file {csv_file} was not found.')
        
        required_columns = ['hour', 'month', 'temp', 'humidity', 'pressure', 'temp_lag_1', 'temp_lag_3', 'temp_target']
        missing_columns = set(required_columns) - set(df.columns)
        if missing_columns:
            raise ValueError(f"Missing required columns: {', '.join(missing_columns)}")
        
        X = df[['hour', 'month', 'temp','humidity','pressure','temp_lag_1','temp_lag_3']]
        y = df['temp_target']
        
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
        
        model.fit(X_train, y_train)

        y_pred = model.predict(X_test)
        mae = mean_absolute_error(y_test, y_pred)
        rmse = root_mean_squared_error(y_test, y_pred)

        log.info(f"Mean Absolute Error: {mae:.2f}°C")
        log.info(f"Root Mean Squared Error: {rmse:.2f}°C")
        
        if mae < 1.5 and rmse < 2:
            status = "Acceptable performance; new model is approved"
            log.info(f'Training complete. {status}')
            joblib.dump(model, model_name)
            log.info(f'Saved new model as {model_name}')
        else:
            status = "Poor performance; new model is discarded."
            log.error(f'Training complete. {status}')

        return {'status': status, 'mae': mae, 'rmse': rmse}
    
    except Exception as e:
        log.error(f'Model training failed! Error: {str(e)}')
        return None
    
""" if __name__ == "__main__":
    try:
        trainModel("weather_forecasting_model_stockholm_xgb.pkl", "processed_weather_data.csv")
    except Exception as e:
        logging.error(f"An error occurred: {str(e)}")
        raise """
