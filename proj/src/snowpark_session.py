from cryptography.hazmat.primitives import serialization
from snowflake.snowpark import Session

with open("../rsa_key.p8", "rb") as key_file:
    private_key = serialization.load_pem_private_key(
        key_file.read(),
        password=None,
    )

connection_parameters = {
    "account": "exqwejz-pmb35848",
    "user": "snowpark_dev",
    "private_key": private_key,
    "role":"spk_dev_role",
    "warehouse":"AOC_WH",
    "database":"AOC2025_DB",
    "schema":"AOC",
}

def connect_to_snowflake():
    session = Session.builder.configs(connection_parameters).create()
    return session

if __name__ == "__main__":
    session = connect_to_snowflake()
    print(session)