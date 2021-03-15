import prefect
from prefect import task, Flow

@task
def say_hello():
    logger = prefect.context.get("logger")
    logger.info("Hello Thomas")

with Flow("hello-flow") as flow:
    say_hello()

flow.register(project_name="tutorial")

def load_all_available_data(path: str) -> pd.DataFrame:
    dataframes = [pd.read_csv(os.path.join(path, name, INPUT_FILENAME)) for name in os.listdir(path)]
    return pd.concat(dataframes, ignore_index=True)

data = load_all_available_data(os.path.join(DATA_DIR, INPUT_DIRNAME))
