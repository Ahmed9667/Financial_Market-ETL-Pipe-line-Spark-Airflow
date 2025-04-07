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
```bash
#Step-by-Step Fix for PySpark with Airflow using Java 17
sudo apt update
sudo apt install openjdk-17-jre-headless
```
