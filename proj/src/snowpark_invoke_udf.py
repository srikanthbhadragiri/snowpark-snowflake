from snowpark_session import connect_to_snowflake

if __name__ == "__main__":
    session = connect_to_snowflake()
    session.sql('''SELECT SNOWPARK_MULTIPLY_TWO_INTEGERS(10,20)''').show()
    session.sql('''SELECT NAME, SNOWPARK_LAST_NAME_FINDER(NAME) as LASTNAME FROM SAMPLE_EMPLOYEE_DATA''').show()