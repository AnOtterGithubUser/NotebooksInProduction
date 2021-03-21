from prefect import task, Flow
from prefect.schedules import IntervalSchedule
import papermill as pm
import datetime



schedule = IntervalSchedule(
    start_date=datetime.datetime.utcnow() + datetime.timedelta(seconds=10),
    interval=datetime.timedelta(days=1)
)


@task(max_retries=3, retry_delay=datetime.timedelta(seconds=10))
def fetch_data():
    pm.execute_notebook("./01_fetch_data.ipynb",
                        "./outputs/01_fetch_data_%s.ipynb" % datetime.datetime.now().strftime("%Y-%m-%d-%H-%M-%S"),
                        parameters={"BTC_TICKER":"BTC-USD",
                                    "NB_DAYS":0,
                                    "DATE_FORMAT":"%Y-%m-%d",
                                    "DATA_DIR":"data",
                                    "OUTPUT_DIRNAME":"01_raw",
                                    "OUTPUT_FILENAME":"raw_data.csv",
                                    "EXECUTION_DATE":datetime.datetime.now().strftime("%Y-%m-%d")})
    
@task
def transform_data(upstream_task):
    pm.execute_notebook("./02_transform_data.ipynb",
                        "./outputs/02_transform_data_%s.ipynb" % datetime.datetime.now().strftime("%Y-%m-%d-%H-%M-%S"),
                        parameters={"BTC_TICKER": "BTC-USD",
                                    "DATA_DIR": "data",
                                    "INPUT_DIRNAME": "01_raw",
                                    "OUTPUT_DIRNAME": "02_clean",
                                    "DATE_FORMAT": "%Y-%m-%d",
                                    "PRICE_COLUMN": "Close",
                                    "DATE_COLUMN": "Date",
                                    "INPUT_FILENAME": "raw_data.csv",
                                    "OUTPUT_FILENAME": "clean_data.csv",
                                    "EXECUTION_DATE": datetime.datetime.now().strftime("%Y-%m-%d")})
    
@task
def predict(upstream_task):
    pm.execute_notebook("./03_predictions.ipynb",
                        "./outputs/03_predictions_%s.ipynb" % datetime.datetime.now().strftime("%Y-%m-%d-%H-%M-%S"),
                        parameters={"MODEL_PATH": "artifacts/2021-03-14/",
                                    "MODEL_NAME": "LinearRegression",
                                    "MODEL_EXTENSION": ".joblib",
                                    "DATA_DIR": "data",
                                    "DATE_FORMAT": "%Y-%m-%d",
                                    "INPUT_DIRNAME": "02_clean",
                                    "INPUT_FILENAME": "clean_data.csv",
                                    "OUTPUT_DIRNAME": "03_predictions",
                                    "OUTPUT_FILENAME": "predictions.csv",
                                    "DAY_PLUS_1": "DAY_PLUS_1",
                                    "DAY_PLUS_7": "DAY_PLUS_7",
                                    "DAY_PLUS_30": "DAY_PLUS_30",
                                    "EXECUTION_DATE": datetime.datetime.now().strftime("%Y-%m-%d"),
                                    "NB_DAYS": 5})
    
@task
def compute_metrics(upstream_task):
    pm.execute_notebook("./04_metrics.ipynb",
                        "./outputs/04_metrics_%s.ipynb" % datetime.datetime.now().strftime("%Y-%m-%d-%H-%M-%S"),
                        parameters={"EXECUTION_DATE": datetime.datetime.now().strftime("%Y-%m-%d"),
                                    "DATA_DIR": "data",
                                    "INPUT_DIRNAME": "03_predictions",
                                    "INPUT_FILENAME": "predictions.csv",
                                    "GROUNDTRUTH_FILENAME": "clean_data.csv",
                                    "DATE_FORMAT": "%Y-%m-%d",
                                    "DAY_PLUS_1": "DAY_PLUS_1",
                                    "DAY_PLUS_7": "DAY_PLUS_7",
                                    "DAY_PLUS_30": "DAY_PLUS_30",
                                    "NB_LAST_DAYS": 30})


with Flow("btc_pipeline", schedule=schedule) as flow:
    data = fetch_data()
    transformed_data = transform_data(data)
    predictions = predict(transformed_data)
    metrics = compute_metrics(predictions)
    
flow.run()
