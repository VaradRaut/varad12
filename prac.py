import pandas as pd
import snowflake.connector as sf
import boto3
from snowflake.connector.pandas_tools import write_pandas

service_name = 's3'
region_name = 'ap-south-1'
aws_access_key_id = 'AKIAUXDLXFKVHXQE6XP4'
aws_secret_access_key = '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'

s3 = boto3.resource(
    service_name=service_name,
    region_name=region_name,
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key
)

user = "avdgroup"
password = "Avd@12345"
account = "vt89313.ap-south-1.aws";
database = "avd"
warehouse = "COMPUTE_WH"
schema = "PUBLIC"
role = "SYSADMIN"

conn = sf.connect(user=user, password=password, account=account);

def run_query(conn, query):
    cursor = conn.cursor();
    cursor.execute(query);
    cursor.close();

statement_1 = 'use warehouse ' + warehouse;
statement2 = 'use database ' + database;
statement3 = 'use role ' + role;

run_query(conn, statement_1)
run_query(conn, statement2)
run_query(conn, statement3)

for obj in s3.Bucket('avdaddress123').objects.all():
    df = pd.read_csv(obj.get()['Body'])
    df.columns = ['P_ADD', 'AREA', 'PIN', 'CT_ID'];
    write_pandas(conn, df, 'ADDRESS')
    print(df)


