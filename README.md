# Financial Market ETL-Pipe-line

## Objective:
Develop a pipeline to analyze financial market data to improve our data-driven
insights.

## Key Stack:
- PySpark
- Apache Airflow

## Project's Tasks:
- Data Extraction: Extract historical and real-time data from financial APIs such as Alpha Vantage and Yahoo Finance. This will enable us to have a comprehensive dataset for analysis.

- Data Transformation: Use Apache Spark to perform data transformations and calculations. This step ensures that the data is in a usable format for analysis and reporting.

- Data Loading: Load the processed data into a relational database such as PostgreSQL. This centralized data repository will support various analytical and reporting needs.
  
- Pipeline Automation: Automate the pipeline to run at regular intervals using Apache Airflow. This ensures continuous data updates and reduces manual intervention.

  ### Data Structure:
  
  ![image](https://github.com/user-attachments/assets/e304e1fc-ce88-4608-93a2-3ae25f9ecb62)

## Notes:
Before Runing Airflow Dag must make sure dependencies of Linux Environment are existed:

### 1.Java:
#### Step-by-Step Fix for PySpark with Airflow using Java 17
```bash
sudo apt update
sudo apt install openjdk-17-jre-headless
```
#### Set JAVA_HOME for Java 17
```bash
sudo update-alternatives --config java
```

#### then:
```bash
echo 'export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64' >> ~/.bashrc
echo 'export PATH=$JAVA_HOME/bin:$PATH' >> ~/.bashrc
source ~/.bashrc
```

#### Verify:
```bash
echo $JAVA_HOME
java -version
```

### 2.PostgreSQL:
#### Check PostgreSQL Status
```bash
sudo service postgresql status
```

#### Create database we use (vantage)
```bash
CREATE DATABASE vantage;
\q
```

![image](https://github.com/user-attachments/assets/945d3141-761c-4379-83cf-177c1b31f4a9)


## Airflow Dag Running:

![image](https://github.com/user-attachments/assets/7a0108f4-7195-48c0-b63a-7a504ae77dc2)

```bash
DESKTOP-F360KDT.
 INFO - ::group::Log message source details
*** Found local files:
***   * /home/ahly9667/airflow/logs/dag_id=vantage/run_id=manual__2025-04-07T20:56:22.502515+00:00/task_id=run_loading/attempt=1.log
 INFO - ::endgroup::
[2025-04-07T22:56:26.912+0200] {local_task_job_runner.py:123} INFO - ::group::Pre task execution logs
[2025-04-07T22:56:26.919+0200] {taskinstance.py:2614} INFO - Dependencies all met for dep_context=non-requeueable deps ti=<TaskInstance: vantage.run_loading manual__2025-04-07T20:56:22.502515+00:00 [queued]>
[2025-04-07T22:56:26.922+0200] {taskinstance.py:2614} INFO - Dependencies all met for dep_context=requeueable deps ti=<TaskInstance: vantage.run_loading manual__2025-04-07T20:56:22.502515+00:00 [queued]>
[2025-04-07T22:56:26.923+0200] {taskinstance.py:2867} INFO - Starting attempt 1 of 2
[2025-04-07T22:56:26.938+0200] {taskinstance.py:2890} INFO - Executing <Task(PythonOperator): run_loading> on 2025-04-07 20:56:22.502515+00:00
[2025-04-07T22:56:26.945+0200] {standard_task_runner.py:104} INFO - Running: ['airflow', 'tasks', 'run', 'vantage', 'run_loading', 'manual__2025-04-07T20:56:22.502515+00:00', '--job-id', '192', '--raw', '--subdir', 'DAGS_FOLDER/dag.py', '--cfg-path', '/tmp/tmpb3n4vgma']
[2025-04-07T22:56:26.946+0200] {standard_task_runner.py:105} INFO - Job 192: Subtask run_loading
[2025-04-07T22:56:26.951+0200] {logging_mixin.py:190} WARNING - /home/ahly9667/my_env/lib/python3.12/site-packages/airflow/task/task_runner/standard_task_runner.py:70 DeprecationWarning: This process (pid=12735) is multi-threaded, use of fork() may lead to deadlocks in the child.
[2025-04-07T22:56:26.952+0200] {standard_task_runner.py:72} INFO - Started process 12749 to run task
[2025-04-07T22:56:27.180+0200] {task_command.py:467} INFO - Running <TaskInstance: vantage.run_loading manual__2025-04-07T20:56:22.502515+00:00 [running]> on host DESKTOP-F360KDT.
[2025-04-07T22:56:27.233+0200] {taskinstance.py:3134} INFO - Exporting env vars: AIRFLOW_CTX_DAG_OWNER='airflow' AIRFLOW_CTX_DAG_ID='vantage' AIRFLOW_CTX_TASK_ID='run_loading' AIRFLOW_CTX_EXECUTION_DATE='2025-04-07T20:56:22.502515+00:00' AIRFLOW_CTX_TRY_NUMBER='1' AIRFLOW_CTX_DAG_RUN_ID='manual__2025-04-07T20:56:22.502515+00:00'
[2025-04-07T22:56:27.234+0200] {logging_mixin.py:190} INFO - Task instance is in running state
[2025-04-07T22:56:27.234+0200] {logging_mixin.py:190} INFO -  Previous state of the Task instance: queued
[2025-04-07T22:56:27.234+0200] {logging_mixin.py:190} INFO - Current task name:run_loading state:running start_date:2025-04-07 20:56:26.919426+00:00
[2025-04-07T22:56:27.234+0200] {logging_mixin.py:190} INFO - Dag name:vantage and current dag run status:running
[2025-04-07T22:56:27.235+0200] {taskinstance.py:732} INFO - ::endgroup::
[2025-04-07T22:56:33.783+0200] {logging_mixin.py:190} INFO - +----------+-------+------+--------+------+--------------+--------+---------------+
| timestamp|   open|  high|     low| close|adjusted close|  volume|dividend amount|
+----------+-------+------+--------+------+--------------+--------+---------------+
|2025-04-04| 242.74|252.79|  226.88|227.48|        227.48|28005665|            0.0|
|2025-03-28| 247.31|254.32|  242.07| 244.0|         244.0|18354282|            0.0|
|2025-03-21| 249.25|254.63| 237.224|243.87|        243.87|27866866|            0.0|
|2025-03-14| 261.56|266.45|  241.68|248.35|        248.35|25513710|            0.0|
|2025-03-07|254.735|261.96|245.1823|261.54|        261.54|22284160|            0.0|
+----------+-------+------+--------+------+--------------+--------+---------------+
only showing top 5 rows
[2025-04-07T22:56:33.786+0200] {logging_mixin.py:190} INFO - root
 |-- timestamp: date (nullable = true)
 |-- open: double (nullable = true)
 |-- high: double (nullable = true)
 |-- low: double (nullable = true)
 |-- close: double (nullable = true)
 |-- adjusted close: double (nullable = true)
 |-- volume: integer (nullable = true)
 |-- dividend amount: double (nullable = true)
[2025-04-07T22:56:34.163+0200] {logging_mixin.py:190} INFO - ✅ Row count: 1326
[2025-04-07T22:56:34.355+0200] {logging_mixin.py:190} INFO - 0
[2025-04-07T22:56:34.505+0200] {logging_mixin.py:190} INFO - 0
[2025-04-07T22:56:34.631+0200] {logging_mixin.py:190} INFO - 0
[2025-04-07T22:56:34.756+0200] {logging_mixin.py:190} INFO - 0
[2025-04-07T22:56:34.870+0200] {logging_mixin.py:190} INFO - 0
[2025-04-07T22:56:34.985+0200] {logging_mixin.py:190} INFO - 0
[2025-04-07T22:56:35.103+0200] {logging_mixin.py:190} INFO - 0
[2025-04-07T22:56:35.220+0200] {logging_mixin.py:190} INFO - 0
[2025-04-07T22:56:35.441+0200] {logging_mixin.py:190} INFO - +----------+-------+------+--------+------+--------------+--------+---------------+
|      date|   open|  high|     low| close|adjusted_close|  volume|dividend_amount|
+----------+-------+------+--------+------+--------------+--------+---------------+
|2025-04-04| 242.74|252.79|  226.88|227.48|        227.48|28005665|            0.0|
|2025-03-28| 247.31|254.32|  242.07| 244.0|         244.0|18354282|            0.0|
|2025-03-21| 249.25|254.63| 237.224|243.87|        243.87|27866866|            0.0|
|2025-03-14| 261.56|266.45|  241.68|248.35|        248.35|25513710|            0.0|
|2025-03-07|254.735|261.96|245.1823|261.54|        261.54|22284160|            0.0|
+----------+-------+------+--------+------+--------------+--------+---------------+
only showing top 5 rows
[2025-04-07T22:56:35.442+0200] {logging_mixin.py:190} INFO - root
 |-- date: date (nullable = true)
 |-- open: float (nullable = true)
 |-- high: float (nullable = true)
 |-- low: float (nullable = true)
 |-- close: float (nullable = true)
 |-- adjusted_close: float (nullable = true)
 |-- volume: long (nullable = true)
 |-- dividend_amount: float (nullable = true)
[2025-04-07T22:56:36.212+0200] {logging_mixin.py:190} INFO - ✅ Data successfully written to PostgreSQL
[2025-04-07T22:56:36.386+0200] {logging_mixin.py:190} INFO - ✅ Inserted row count: 1326
[2025-04-07T22:56:36.494+0200] {logging_mixin.py:190} INFO - +----------+------------------+------------------+------------------+------------------+------------------+--------+---------------+
|      date|              open|              high|               low|             close|    adjusted_close|  volume|dividend_amount|
+----------+------------------+------------------+------------------+------------------+------------------+--------+---------------+
|2025-04-04|242.74000549316406| 252.7899932861328| 226.8800048828125|227.47999572753906|227.47999572753906|28005665|            0.0|
|2025-03-28|247.30999755859375|254.32000732421875|242.07000732421875|             244.0|             244.0|18354282|            0.0|
|2025-03-21|            249.25| 254.6300048828125| 237.2239990234375| 243.8699951171875| 243.8699951171875|27866866|            0.0|
|2025-03-14|261.55999755859375|266.45001220703125|241.67999267578125|248.35000610351562|248.35000610351562|25513710|            0.0|
|2025-03-07|254.73500061035156| 261.9599914550781| 245.1822967529297| 261.5400085449219| 261.5400085449219|22284160|            0.0|
+----------+------------------+------------------+------------------+------------------+------------------+--------+---------------+
only showing top 5 rows
[2025-04-07T22:56:36.495+0200] {python.py:240} INFO - Done. Returned value was: None
[2025-04-07T22:56:36.501+0200] {taskinstance.py:341} INFO - ::group::Post task execution logs
[2025-04-07T22:56:36.502+0200] {taskinstance.py:353} INFO - Marking task as SUCCESS. dag_id=vantage, task_id=run_loading, run_id=manual__2025-04-07T20:56:22.502515+00:00, execution_date=20250407T205622, start_date=20250407T205626, end_date=20250407T205636
[2025-04-07T22:56:36.528+0200] {logging_mixin.py:190} INFO - Task instance in success state
[2025-04-07T22:56:36.528+0200] {logging_mixin.py:190} INFO -  Previous state of the Task instance: running
[2025-04-07T22:56:36.528+0200] {logging_mixin.py:190} INFO - Dag name:vantage queued_at:2025-04-07 20:56:22.509950+00:00
[2025-04-07T22:56:36.528+0200] {logging_mixin.py:190} INFO - Task hostname:DESKTOP-F360KDT. operator:PythonOperator
[2025-04-07T22:56:36.554+0200] {local_task_job_runner.py:266} INFO - Task exited with return code 0
[2025-04-07T22:56:36.562+0200] {taskinstance.py:3901} INFO - 0 downstream tasks scheduled from follow-on schedule check
[2025-04-07T22:56:36.563+0200] {local_task_job_runner.py:245} INFO - ::endgroup::
```

![image](https://github.com/user-attachments/assets/cc63cc58-1011-43ce-80d2-4ae7ec03a2e5)

## Check For Loaded database:
#### Access to postgresql:
```bash
sudo -u postgres psql
```

#### list databases:
```bash
\l
```

#### Connect to Your Database:
```bash
\c vantage
```

#### list tables:
```bash
\dt
```

#### Check for inserted records:
```bash
SELECT COUNT(*) FROM stock;
```

![image](https://github.com/user-attachments/assets/37dc30f1-d8ec-41f8-bf00-35a3c31de1fa)




