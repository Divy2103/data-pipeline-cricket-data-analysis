import pandas as pd
import requests
import snowflake.connector

# Function to fetch player data
def fetch_player_data(player_id):
    url = f"https://cricbuzz-cricket.p.rapidapi.com/stats/v1/player/{player_id}/batting"
    # headers = {'Authorization': 'Bearer your_token'}  # Replace with your actual headers
    headers = {
            "x-rapidapi-key": f"6567132011mshed68849c12b0db0p10f15ejsnd58ad5ce4767",
            "x-rapidapi-host": "cricbuzz-cricket.p.rapidapi.com"
    }
    response = requests.get(url, headers=headers)
    response.raise_for_status()  # Check for HTTP errors
    return response.json()

# Function to process the data
def process_data(data, player_id, player_name):
    headers = data['headers'][1:]  # Skip 'ROWHEADER'
    values = data['values']
    
    records = []
    for i, format in enumerate(headers):
        
        record = {}
        
        record['PlayerID'] = player_id
        record['Format'] = format
        record['PlayerName'] = player_name
        
        for _,val in enumerate(values):
            print(f'{val['values'][0]} - {val['values'][i + 1]}')
            col_name = val['values'][0]
            record[col_name] = float(val['values'][i+1]) if '.' in val['values'][i+1] else int(val['values'][i+1])
            
        df1 = pd.DataFrame([record])
        print(df1)
        # record = {
        #     'PlayerID': player_id,
        #     'PlayerName': player_name,
        #     'Format': format,
        #     'Matches': int(values[0]['values'][i+1]),
        #     'Innings': int(values[1]['values'][i+1]),
        #     'Runs': int(values[2]['values'][i+1]),
        #     'Balls': int(values[3]['values'][i+1]),
        #     'Highest': int(values[4]['values'][i+1]),
        #     'Average': float(values[5]['values'][i+1]),
        #     'SR': float(values[6]['values'][i+1]),
        #     'NotOut': int(values[7]['values'][i+1]),
        #     'Fours': int(values[8]['values'][i+1]),
        #     'Sixes': int(values[9]['values'][i+1]),
        #     'Ducks': int(values[10]['values'][i+1]),
        #     'Fifties': int(values[11]['values'][i+1]),
        #     'Hundreds': int(values[12]['values'][i+1]),
        #     'DoubleHundreds': int(values[13]['values'][i+1]),
        #     'TripleHundreds': int(values[14]['values'][i+1]),
        #     'QuadrupleHundreds': int(values[15]['values'][i+1])
        # }
        # records.append(record)
    # return records

# Fetch and process data for multiple players
player_ids = [1413]  # Example player IDs
all_records = []

for player_id in player_ids:
    
    data = fetch_player_data(player_id)
    player_name = data['appIndex']['seoTitle'].split(' - ')[0]  # Extract player name
    # records = process_data(data, player_id, player_name)
    # all_records.extend(records)

# Create a DataFrame
# df = pd.DataFrame(all_records)
# print(df.to_string())

# Connect to Snowflake
# conn = snowflake.connector.connect(
#     user='your_username',
#     password='your_password',
#     account='your_account'
# )

# Create a cursor object
# cursor = conn.cursor()

# # Create the table in Snowflake
# create_table_query = """
# CREATE TABLE IF NOT EXISTS PlayerStats (
#     PlayerID INT,
#     PlayerName STRING,
#     Format STRING,
#     Matches INT,
#     Innings INT,
#     Runs INT,
#     Balls INT,
#     Highest INT,
#     Average FLOAT,
#     SR FLOAT,
#     NotOut INT,
#     Fours INT,
#     Sixes INT,
#     Ducks INT,
#     Fifties INT,
#     Hundreds INT,
#     DoubleHundreds INT,
#     TripleHundreds INT,
#     QuadrupleHundreds INT
# )
# """
# cursor.execute(create_table_query)

# # Insert data into the table
# for index, row in df.iterrows():
#     insert_query = f"""
#     INSERT INTO PlayerStats (
#         PlayerID, PlayerName, Format, Matches, Innings, Runs, Balls, Highest, Average, SR, NotOut, Fours, Sixes, Ducks, Fifties, Hundreds, DoubleHundreds, TripleHundreds, QuadrupleHundreds
#     ) VALUES (
#         {row['PlayerID']}, '{row['PlayerName']}', '{row['Format']}', {row['Matches']}, {row['Innings']}, {row['Runs']}, {row['Balls']}, {row['Highest']}, {row['Average']}, {row['SR']}, {row['NotOut']}, {row['Fours']}, {row['Sixes']}, {row['Ducks']}, {row['Fifties']}, {row['Hundreds']}, {row['DoubleHundreds']}, {row['TripleHundreds']}, {row['QuadrupleHundreds']}
#     )
#     """
#     cursor.execute(insert_query)

# # Commit the transaction
# conn.commit()

# # Close the connection
# cursor.close()
# conn.close()