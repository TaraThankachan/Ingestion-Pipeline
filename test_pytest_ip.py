import pytest

from lib import DataLoader
from lib.Utils import get_spark_session


from pyspark.sql.types import StructType, StructField, StringType, NullType, TimestampType, ArrayType, DateType, Row

@pytest.fixture(scope='session')
def spark():
    return get_spark_session("LOCAL")


@pytest.fixture(scope='session')

def expected_company_rows():
    return [Row(employeeId=61, employee_name='Tara',
                company_name='SEB', joining_date='2018-04-25'),
            Row(employeeId=107, employee_name='John',
                company_name='Volvo', joining_date='2022-05-21'),
            Row(employeeId=95, employee_name='Shruthi',
                company_name='SwedBank', joining_date='2021-03-02'),
            Row(employeeId=26, employee_name='Maheesh',
                company_name='ICA', joining_date='2020-12-12'),
            Row(employeeId=122, employee_name='Mithul',
                company_name='Ikea', joining_date='2023-03-16'),
            Row(employeeId=124, employee_name='Rohini',
                company_name='Volvo', joining_date='2021-09-23'),
            Row(employeeId=28, employee_name='Rageesh',
                company_name='SEB', joining_date='2018-06-25'),
            Row(employeeId=11, employee_name='Traicy',
                company_name='ICA', joining_date='2019-11-10')]


@ pytest.fixture(scope='session')

def expected_employee_rows():
    return [Row(employeeId=61, age=32,
                sex='F', marital_status='Married'),
            Row(employeeId=107, age=36,
                 sex='M', marital_status='Married'),
            Row(employeeId=95, age=24,
                sex='F', marital_status='Single'),
            Row(employeeId= 26, age= 26,
               sex='M', marital_status='Married'),
            Row(employeeId=122, age=30,
                sex='M', marital_status='Single'),
            Row(employeeId=124, age=31,
                sex='F', marital_status='Single'),
            Row(employeeId=28, age=38,
                sex='M', marital_status='Single'),
            Row(employeeId=11, age=26,
                sex='F', marital_status='Single')]



def test_blank_test(spark):
    print(spark.version)
    assert spark.version == "3.4.0"
def test_salary(spark):
    salary_df = DataLoader.load_salary_details(spark, 'LOCAL')
    assert salary_df.count() == 8

def test_read_employee_row(spark, expected_employee_rows):
    actual_employee_rows = DataLoader.load_employee_details(spark, "LOCAL").collect()
    assert expected_employee_rows == actual_employee_rows


