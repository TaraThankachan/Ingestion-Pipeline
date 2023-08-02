


def join_company_employee(company_df,employee_df):
    join_df = company_df.join(employee_df, ['employeeId'], "outer")
    return join_df

def salary_band_df(salary_df):
    return salary_df.withColumn('FinalSalary',salary_df.salary*salary_df.band)

def join_final(ce_df, s_df):

    join_df = ce_df.join(s_df,ce_df.employeeId == s_df.employeeId,"outer")
    join_df.show()
    return join_df

