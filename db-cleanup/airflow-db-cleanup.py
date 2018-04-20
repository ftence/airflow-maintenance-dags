from airflow.models import DAG, DagRun, TaskInstance, TaskFail, Log, XCom, SlaMiss
from airflow.jobs import BaseJob
from airflow.models import settings
from airflow.operators.python_operator import PythonOperator
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import (
    Column, Integer, String, DateTime, Text, Boolean, ForeignKey, PickleType,
    Index, Float, LargeBinary)
from datetime import datetime, timedelta
import os
import logging

"""
A maintenance workflow that you can deploy into Airflow to periodically clean out the DagRun, TaskInstance, TaskFail, Log, XCom, Job DB and SlaMiss entries to avoid having too much data in your Airflow MetaStore.

airflow trigger_dag --conf '{"maxDBEntryAgeInDays":30}' airflow-db-cleanup

--conf options:
    maxDBEntryAgeInDays:<INT> - Optional

"""

Base = declarative_base()


class Job(Base):
    __tablename__ = "job"

    id = Column(Integer, primary_key=True)
    dag_id = Column(String(250))
    state = Column(String(20))
    job_type = Column(String(30))
    start_date = Column(DateTime)
    end_date = Column(DateTime)
    latest_heartbeat = Column(DateTime)
    executor_class = Column(String(500))
    hostname = Column(String(500))
    unixname = Column(String(1000))

    def __repr__(self):
        return str((
            self.dag_id, self.hostname, self.start_date.isoformat()))


class CeleryTaskMeta(Base):
    __tablename__ = "celery_taskmeta"

    id = Column(Integer, primary_key=True)
    task_id = Column(String(155))
    status = Column(String(50))
    result = Column(LargeBinary)
    date_done = Column(DateTime)
    traceback = Column(Text)

    def __repr__(self):
        return str((
            self.task_id, self.status, self.date_done.isoformat()))


DAG_ID = os.path.basename(__file__).replace(".pyc", "").replace(".py", "")  # airflow-db-cleanup
START_DATE = datetime(2018, 4, 15, 0, 0)
SCHEDULE_INTERVAL = "@daily"  # How often to Run. @daily - Once a day at Midnight (UTC)
DAG_OWNER_NAME = "operations"  # Who is listed as the owner of this DAG in the Airflow Web Server
DEFAULT_MAX_DB_ENTRY_AGE_IN_DAYS = 30  # Length to retain the log files if not already provided in the conf. If this is set to 30, the job will remove those files that are 30 days old or older.
ENABLE_DELETE = True  # Whether the job should delete the db entries or not. Included if you want to temporarily avoid deleting the db entries.
DATABASE_OBJECTS = [  # List of all the objects that will be deleted. Comment out the DB objects you want to skip.
    {"airflow_db_model": DagRun, "age_check_column": DagRun.execution_date},
    # Be careful with that, will cause a massive backfill if DAG disabled/enabled
    {"airflow_db_model": TaskInstance, "age_check_column": TaskInstance.execution_date},
    {"airflow_db_model": TaskFail, "age_check_column": TaskFail.execution_date},
    {"airflow_db_model": Log, "age_check_column": Log.dttm},
    {"airflow_db_model": XCom, "age_check_column": XCom.execution_date},
    {"airflow_db_model": BaseJob, "age_check_column": BaseJob.latest_heartbeat},
    {"airflow_db_model": SlaMiss, "age_check_column": SlaMiss.execution_date},
    {"airflow_db_model": Job, "age_check_column": Job.latest_heartbeat},
    {"airflow_db_model": CeleryTaskMeta, "age_check_column": CeleryTaskMeta.date_done},
]

session = settings.Session()


def cleanup_function(**context):
    logging.info("Loading Configurations...")
    dag_run_conf = context.get("dag_run").conf
    logging.info("dag_run.conf: " + str(dag_run_conf))
    max_db_entry_age_in_days = None
    if dag_run_conf:
        max_db_entry_age_in_days = dag_run_conf.get("maxDBEntryAgeInDays", None)
    logging.info("maxDBEntryAgeInDays from dag_run.conf: " + str(dag_run_conf))
    if max_db_entry_age_in_days is None:
        logging.info("maxDBEntryAgeInDays conf variable isn't included. Using Default '" + str(
            DEFAULT_MAX_DB_ENTRY_AGE_IN_DAYS) + "'")
        max_db_entry_age_in_days = DEFAULT_MAX_DB_ENTRY_AGE_IN_DAYS
    max_date = datetime.utcnow() + timedelta(days=-max_db_entry_age_in_days)

    airflow_db_model = context["params"].get("airflow_db_model")
    age_check_column = context["params"].get("age_check_column")

    logging.info("Configurations:")
    logging.info("max_db_entry_age_in_days: " + str(max_db_entry_age_in_days))
    logging.info("max_date:                 " + str(max_date))
    logging.info("enable_delete:            " + str(ENABLE_DELETE))
    logging.info("session:                  " + str(session))
    logging.info("airflow_db_model:         " + str(airflow_db_model))
    logging.info("age_check_column:         " + str(age_check_column))
    logging.info("")

    logging.info("Running Cleanup Process...")

    entries_to_delete = session.query(airflow_db_model).filter(
        age_check_column <= max_date,
    ).all()
    logging.info("Process will be Deleting the following " + str(airflow_db_model.__name__) + "(s):")
    for entry in entries_to_delete:
        logging.info("\tEntry: " + str(entry) + ", Date: " + str(entry.__dict__[str(age_check_column).split(".")[1]]))
    logging.info(
        "Process will be Deleting " + str(len(entries_to_delete)) + " " + str(airflow_db_model.__name__) + "(s)")

    if ENABLE_DELETE:
        logging.info("Performing Delete...")
        for entry in entries_to_delete:
            session.delete(entry)
        logging.info("Finished Performing Delete")
        session.commit()
    else:
        logging.warning("You're opted to skip deleting the db entries!!!")

    logging.info("Finished Running Cleanup Process")


default_args = {
    'owner': DAG_OWNER_NAME,
    'email_on_failure': False,
    'email_on_retry': False,
    'start_date': START_DATE,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(DAG_ID, default_args=default_args, schedule_interval=SCHEDULE_INTERVAL, start_date=START_DATE)

for db_object in DATABASE_OBJECTS:
    cleanup = PythonOperator(
        task_id='cleanup_' + str(db_object["airflow_db_model"].__name__),
        python_callable=cleanup_function,
        params=db_object,
        provide_context=True,
        dag=dag
    )
