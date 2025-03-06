from snowpark_session import connect_to_snowflake
from snowflake.snowpark.functions import col

def create_table(session):
    # create table using session obj
    session.sql('CREATE OR REPLACE TABLE SAMPLE_EMPLOYEE_DATA (id INT,name VARCHAR, age INT, email VARCHAR, city VARCHAR,country VARCHAR)').collect()
    session.sql('SHOW TABLES')

def insert_data(session):
    session.sql("""
        INSERT INTO SAMPLE_EMPLOYEE_DATA VALUES
        (1,'John Doe',25,'johndoe@example.com','New York','USA'),
        (2,'Jane Smith',30,'janesmith@example.com','Los Angeles','USA'),
        (3,'Michael Johnson',35,'michaeljohnson@example.com','London', 'UK'),
        (4,'Sarah Williams',28,'sarahwilliams@example.com','Leeds', 'UK'),
        (5,'David Brown',32,'davidbrown@example.com','Tokyo','Japan'),
        (6,'Emily Davis',29,'emilydavis@example.com','Sydney','Australia'),
        (7,'James Miller',27,'jamesmiller@example.com','Dallas','USA'),
        (8,'Emma Wilson',33,'emmawilson@example.com','Berlin','Germany'),
        (9,'Alexander Taylor',31,'alexandertaylor@example.com','Rome','Italy'),
        (10,'Olivia Anderson',26,'oliviaanderson@example.com','Melbourne','Australia')
    """).collect()

def get_data(session):
    session.sql("SELECT count(*) FROM SAMPLE_EMPLOYEE_DATA").collect()

def get_row_by_id(session):
    df_subset_row = session.table("SAMPLE_EMPLOYEE_DATA").filter(col("id") == 1)
    df_subset_row.show()

if __name__ == "__main__":
    session = connect_to_snowflake()

    create_table(session)
    insert_data(session)
    get_data(session)
    get_row_by_id(session)
