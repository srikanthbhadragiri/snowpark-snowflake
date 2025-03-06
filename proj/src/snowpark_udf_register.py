from snowpark_session import connect_to_snowflake
from snowflake.snowpark.types import IntegerType, StringType

# define the function for udf
def multiply_together(input_int_1:int, input_int_2:int):
    return input_int_1*input_int_2
    
def register_multiply_udf(session):
    session.udf.register(
        func = multiply_together,
        return_type=IntegerType(),
        input_types=[IntegerType(), IntegerType()],
        is_permanent=True,
        name='SNOWPARK_MULTIPLY_TWO_INTEGERS',
        replace=True,
        stage_location='@aoc_files_stage'
    )

# UDF for last name finder
# name is inserted as full name "John Doe". Find the last name/word from the full name.
def last_name_finder(input_name:str):
    last_name = input_name.split()[1]
    return last_name

def register_last_name_finder(session):
    session.udf.register(
        func = last_name_finder,
        return_type=StringType(),
        input_types=[StringType()],
        is_permanent=True,
        name='SNOWPARK_LAST_NAME_FINDER',
        replace=True,
        stage_location='@aoc_files_stage'
    )

if __name__ == "__main__":
    session = connect_to_snowflake()
    print(session)
    register_multiply_udf(session)
    register_last_name_finder(session)
    