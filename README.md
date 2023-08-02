# PySpark Example Project

## Use Case Overveiw
The aim of the project is to load three tables: company_details,employee_details and salary_details
into raw layer.It is expected to transform and join these tables to calculate final salary along with other details.
Frequency: daily
Expected number of records per day:20
Load type:Full refresh
Data Consumption Pattern by downstream:partion by loaddate
## ETL Project Structure

The basic project structure is as follows:

```bash
root/
 |-- configs/
 |   |-- env_config
 |   |-- spark_config
 |-- lib/
 |   |-- __init__.py
 |   |-- ConfigLoader.py
 |   |-- DataLoader.py
 |   |-- logger.py
 |   |-- Tranformation.py
 |   |-- Utils.py
 |-- Test_data/
 |   |-- etl_job.py
 |-- etl_main/
 |-- etl_submit.sh/
 |-- log$j.properties/
 |   Pipfile
 |   Pipfile.lock
  |   test_pytest_etl.py
```
