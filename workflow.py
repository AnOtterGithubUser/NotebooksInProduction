from prefect import task, Flow
from prefect.schedules import IntervalSchedule
import papermill as pm
import datetime
import os



schedule = IntervalSchedule(
    start_date=datetime.datetime.utcnow() + datetime.timedelta(seconds=5),
    interval=datetime.timedelta(days=1)
)


@task(max_retries=3, retry_delay=datetime.timedelta(seconds=10))
def fetch_data():
    pm.execute_notebook("./01_fetch_data.ipynb",
                        "./01_fetch_data_%s.ipynb" % datetime.datetime.now().strftime("%Y-%m-%d"),
                        parameters={"BTC_TICKER":"BTC-USD",
                                    "NB_DAYS":0,
                                    "DATE_FORMAT":"%Y-%m-%d",
                                    "DATA_DIR":"data",
                                    "OUTPUT_DIRNAME":"01_raw",
                                    "OUTPUT_FILENAME":"raw_data.csv",
                                    "EXECUTION_DATE": datetime.datetime.now().strftime("%Y-%m-%d")
                                   })
    
@task
def transform_data(upstream_task):
    pm.execute_notebook("./02_transform_data.ipynb",
                        "./02_transform_data_%s.ipynb" % datetime.datetime.now().strftime("%Y-%m-%d"),
                        parameters={"BTC_TICKER": "BTC-USD",
                                    "DATA_DIR": "data",
                                    "INPUT_DIRNAME": "01_raw",
                                    "OUTPUT_DIRNAME": "02_clean",
                                    "DATE_FORMAT": "%Y-%m-%d",
                                    "PRICE_COLUMN": "Close",
                                    "DATE_COLUMN": "Date",
                                    "INPUT_FILENAME": "raw_data.csv",
                                    "OUTPUT_FILENAME": "clean_data.csv",
                                    "EXECUTION_DATE": datetime.datetime.now().strftime("%Y-%m-%d")
                                   })
    
@task
def predict_linear_regression(upstream_task):
    pm.execute_notebook("./03_predictions.ipynb",
                        "./03_predictions_linear_regression_%s.ipynb" % datetime.datetime.now().strftime("%Y-%m-%d"),
                        parameters={"MODEL_PATH": "artifacts/2021-03-18/",
                                    "MODEL_NAME": "LinearRegression",
                                    "MODEL_EXTENSION": ".joblib",
                                    "DATA_DIR": "data",
                                    "DATE_FORMAT": "%Y-%m-%d",
                                    "INPUT_DIRNAME": "02_clean",
                                    "INPUT_FILENAME": "clean_data.csv",
                                    "OUTPUT_DIRNAME": "03_predictions",
                                    "OUTPUT_FILENAME": "predictions_linear_regression.csv",
                                    "DAY_PLUS_1": "DAY_PLUS_1",
                                    "DAY_PLUS_7": "DAY_PLUS_7",
                                    "DAY_PLUS_30": "DAY_PLUS_30",
                                    "EXECUTION_DATE": datetime.datetime.now().strftime("%Y-%m-%d"),
                                    "NB_DAYS": 7})
    
@task
def predict_gradient_boosting(upstream_task):
    pm.execute_notebook("./03_predictions.ipynb",
                        "./03_predictions_gradient_boosting_%s.ipynb" % datetime.datetime.now().strftime("%Y-%m-%d"),
                        parameters={"MODEL_PATH": "artifacts/2021-03-18/",
                                    "MODEL_NAME": "GradientBoostingRegressor",
                                    "MODEL_EXTENSION": ".joblib",
                                    "DATA_DIR": "data",
                                    "DATE_FORMAT": "%Y-%m-%d",
                                    "INPUT_DIRNAME": "02_clean",
                                    "INPUT_FILENAME": "clean_data.csv",
                                    "OUTPUT_DIRNAME": "03_predictions",
                                    "OUTPUT_FILENAME": "predictions_gradient_boosting.csv",
                                    "DAY_PLUS_1": "DAY_PLUS_1",
                                    "DAY_PLUS_7": "DAY_PLUS_7",
                                    "DAY_PLUS_30": "DAY_PLUS_30",
                                    "EXECUTION_DATE": datetime.datetime.now().strftime("%Y-%m-%d"),
                                    "NB_DAYS": 7})
    
@task
def predict_prophet(upstream_task):
    pm.execute_notebook("./03_predictions_prophet.ipynb",
                        "./03_predictions_prophet_%s.ipynb" % datetime.datetime.now().strftime("%Y-%m-%d"),
                        parameters={"MODEL_PATH": "artifacts/2021-03-18/",
                                    "MODEL_NAME": "prophet.json",
                                    "DATA_DIR": "data",
                                    "DATE_FORMAT": "%Y-%m-%d",
                                    "INPUT_DIRNAME": "02_clean",
                                    "INPUT_FILENAME": "clean_data.csv",
                                    "OUTPUT_DIRNAME": "03_predictions",
                                    "OUTPUT_FILENAME": "predictions_prophet.csv",
                                    "DAY_PLUS_1": "DAY_PLUS_1",
                                    "DAY_PLUS_7": "DAY_PLUS_7",
                                    "DAY_PLUS_30": "DAY_PLUS_30",
                                    "EXECUTION_DATE": datetime.datetime.now().strftime("%Y-%m-%d")
                                   })

@task
def compute_metrics(**upstream_task):
    pm.execute_notebook("./04_metrics.ipynb",
                        "./04_metrics_%s.ipynb" % datetime.datetime.now().strftime("%Y-%m-%d"),
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

@task
def display_webapp(metrics):
    os.system("voila 04_metrics_%s.ipynb" % datetime.datetime.now().strftime("%Y-%m-%d"))


with Flow("btc_pipeline", schedule=schedule) as flow:
    data = fetch_data()
    transformed_data = transform_data(data)
    predictions_linear_regression = predict_linear_regression(transformed_data)
    predictions_gradient_boosting = predict_gradient_boosting(transformed_data)
    predictions_prophet = predict_prophet(transformed_data)
    metrics = compute_metrics(pred_lr=predictions_linear_regression, pred_gb=predictions_gradient_boosting, pred_prophet=predictions_prophet)
    display_webapp(metrics)
    
flow.run()
