import os
import datetime

end_date = datetime.datetime.now()

date = datetime.datetime.strptime("2021-01-01", "%Y-%m-%d")

while(date <= end_date):
    print("DATE: %s" % datetime.datetime.strftime(date, "%Y-%m-%d"))
    os.system("papermill 03_predictions_prophet.ipynb outputs/test3.ipynb -p EXECUTION_DATE %s" % datetime.datetime.strftime(date, "%Y-%m-%d"))
    date += datetime.timedelta(days=1)
