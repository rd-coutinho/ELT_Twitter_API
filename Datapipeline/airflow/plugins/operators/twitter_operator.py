from airflow.models import BaseOperator, DAG, TaskInstance
from airflow.utils.decorators import apply_defaults
from airflow.utils.state import DagRunState
from airflow.utils.types import DagRunType
from hooks.twitter_hook import TwitterHook   # Taking the created hook to the operator
import json 
from datetime import datetime, timedelta
from pathlib import Path
from os.path import join


# DATA_INTERVAL_START = datetime.datetime(2021, 9, 13)
# DATA_INTERVAL_END = DATA_INTERVAL_START + datetime.timedelta(days=1)
DATA_INTERVAL_START = datetime.now()
DATA_INTERVAL_END = DATA_INTERVAL_START

class TwitterOperator(BaseOperator):

    template_fields = [
        "query",
        "file_path",
        "start_time",
        "end_time"
    ]

    @apply_defaults
    def __init__(
        self,
        query,
        file_path,
        conn_id = None,    # It's the name of the connection
        start_time = None,
        end_time = None,
        *args, **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.query = query
        self.file_path = file_path
        self.conn_id = conn_id
        self.start_time = start_time
        self.end_time = end_time

    def create_parent_folder(self):
        Path(Path(self.file_path).parent).mkdir(parents=True, exist_ok=True)

    def execute(self, context):   
        hook = TwitterHook(
            query=self.query,
            conn_id=self.conn_id,
            start_time=self.start_time,
            end_time=self.end_time
        )
        self.create_parent_folder()
        with open(self.file_path, "w") as output_file:  
            for pg in hook.run():
                json.dump(pg, output_file, ensure_ascii=False)   
                output_file.write("\n")  


###############
if __name__ == "__main__":
    with DAG(dag_id="TwitterTest", start_date=datetime.now()) as dag:
        to = TwitterOperator(
            query="AluraOnline",
            file_path=join(
                "/home/rd-coutinho/Documents/Alura/Datapipeline/datalake",
                "twitter_aluraonline",   
                "extract_date={{ ds }}",    
                "AluraOnline_{{ ds_nodash }}.json"
            ),  
            task_id="test_run"
        )

        dagrun = dag.create_dagrun(
            state=DagRunState.RUNNING,
            execution_date=datetime.now(),
            # execution_date=datetime(2022, 6, 24),
            # data_interval=(datetime.now() - timedelta(days=1), datetime.now() - timedelta(days=1)),
            # start_date=datetime.now() - timedelta(days=1),
            run_type=DagRunType.MANUAL,
        )
        
        ti = dagrun.get_task_instance(task_id="test_run")
        ti.task = dag.get_task(task_id="test_run")
        ti.run(ignore_ti_state=True)
