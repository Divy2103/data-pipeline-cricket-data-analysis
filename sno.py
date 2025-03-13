import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from snowflake.connector import connect,DictCursor
import pandas as pd
import requests

conn = snowflake.connector.connect(
 user='project',
 password='Project@123456',
 account='fzkreem-ojb05768',
 warehouse = 'COMPUTE_WH',
 role = 'ACCOUNTADMIN',
)

cs = conn.cursor()
cs.execute('CREATE or REPLACE DATABASE "cricbuzz"')
cs.execute('USE DATABASE "cricbuzz"')

cs.execute('CREATE OR REPLACE TABLE "cricbuzz_table" ("id" number(10),"rank" number(10),"name" varchar(50),'
'"country" varchar(20),"rating" number(5),"difference" number(10,2),"points" number(5),"lastUpdatedOn" timestamptz,"trend" varchar(5),'
'"faceImageId" number(10),"countryId" number(10))')


def get_icc_ranking_player():
    l = [[0,'test'],[0,'odi'],[0,'t20'],[1,'odi'],[1,'t20']]

    for i in l:
        url = f"https://cricbuzz-cricket.p.rapidapi.com/stats/v1/rankings/batsmen?formatType={i[1]}&isWomen={i[0]}"

        headers = {
            "x-rapidapi-key": "4f16ab08e8msh27e320e3d194653p1e0308jsn539b154232c5",
            "x-rapidapi-host": "cricbuzz-cricket.p.rapidapi.com"
        }

        response = requests.get(url, headers=headers)
        data = response.json()
        df = pd.DataFrame(data['rank'])
        df['isWomen'] = 1
        df['formatType'] = 'odi'

        write_pandas(conn,df,table_name="cricbuzz_table")



curr = conn.cursor(DictCursor)
curr.execute('SELECT * FROM "cricbuzz_table"')
data_table = curr.fetchall()
pd.DataFrame(data_table)

print('completed')



# def get_icc_ranking_player():
#     l = [[0, 'test'], [0, 'odi'], [0, 't20'], [1, 'odi'], [1, 't20']]

#     for i in l:
#         url = f"https://cricbuzz-cricket.p.rapidapi.com/stats/v1/rankings/batsmen?formatType={i[1]}&isWomen={i[0]}"

#         headers = {
#             "x-rapidapi-key": "4f16ab08e8msh27e320e3d194653p1e0308jsn539b154232c5",
#             "x-rapidapi-host": "cricbuzz-cricket.p.rapidapi.com"
#         }

#         try:
#             response = requests.get(url, headers=headers)
#             response.raise_for_status()  # Check for HTTP errors
#             data = response.json()
            
#             df = pd.DataFrame(data['rank'])
#             df['isWomen'] = i[0]
#             df['formatType'] = i[1]

#             write_pandas(conn, df, table_name="cricbuzz_table")

#         except requests.exceptions.RequestException as e:
#             print(f"Request failed: {e}")
#         except KeyError as e:
#             print(f"Key error: {e}")
#         except Exception as e:
#             print(f"An error occurred: {e}")