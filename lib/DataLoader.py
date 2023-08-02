def get_employee_schema():
    schema='employeeId int,age int,sex string,marital_status string'
    return schema

def get_company_schema():
    schema='employeeId int,employee_name string, company_name string,joining_date date'
    return schema

def get_salary_schema():
    schema='employeeId int,salary long,band int'
    return schema


def load_employee_details(spark,env):
    if env=='LOCAL':
        return spark.read\
                .format('csv')\
                .option('header','true')\
                .schema(get_employee_schema())\
                .load('test_data/employee')

    #TODO QA and Prod conditions

def load_company_details(spark,env):
    if env=='LOCAL':
        return spark.read\
                .format('csv') \
                 .option('header', 'true') \
                .schema(get_company_schema()) \
                 .load('test_data/company')

        #TODO

def load_salary_details(spark,env):
    if env=='LOCAL':
        return spark.read\
                .format('csv') \
                 .option('header', 'true') \
                .schema(get_salary_schema()) \
                 .load('test_data/salary')
        #TODO


