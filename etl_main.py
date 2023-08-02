import sys
import uuid

from lib import Utils
from lib.logger import Log4j
from lib import DataLoader
from lib import ConfigLoader
from lib import Transformations


if __name__ == '__main__':

    if len(sys.argv) < 3:
        print("Usage: etl {local, qa, prod} {load_date} : Arguments are missing")
        sys.exit(-1)
    job_run_env = sys.argv[1].upper()
    load_date = sys.argv[2]
    job_run_id = "ETL-" + str(uuid.uuid4())

    print("Initializing ETL Job in " + job_run_env + " Job ID: " + job_run_id)
    conf = ConfigLoader.get_config(job_run_env)
    spark=Utils.get_spark_session(job_run_env)
    log=Log4j(spark)
# log that main ETL job is starting
    log.warn('etl_job is up-and-running')
    log.info('Reading Employee DF')
    employee_df = DataLoader.load_employee_details(spark,job_run_env)
    log.info("Reading Company DF")
    company_df = DataLoader.load_company_details(spark,job_run_env)
    log.info('Reading Salary DF')
    salary_df = DataLoader.load_salary_details(spark, job_run_env)
    log.info('Join Company table and employee table')
    company_employee_df = Transformations.join_company_employee(employee_df,company_df)
    log.info('Adding band*salary in salary_df')
    band_sal_df = Transformations.salary_band_df(salary_df)

    log.info('Joining company_employee_df and band_sal_df')
    final_df = Transformations.join_final(company_employee_df,band_sal_df)
    #print(final_df)


