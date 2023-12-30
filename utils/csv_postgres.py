import pandas as pd
from sqlalchemy import create_engine


class csv_to_postgres:


    def is_valid_csv(csv_file_path):
        if len(csv_file_path) < 5:
            return False
        file_extension = csv_file_path[-4:]
        if file_extension != '.csv':
            return False
        try:
            file = open(csv_file_path)
            file.close()        
        except:
            return False
        return True
    
    def test_connection_to_postgres_db(connection_dict: dict):
        postgres_db_conn_str = f'postgresql+psycopg2://{connection_dict["u_name"]}:{connection_dict["pass"]}@{connection_dict["host"]}:{connection_dict["port"]}/{connection_dict["db_name"]}'
        engine = create_engine(postgres_db_conn_str)
        try:
            connection = engine.connect()
            connection.close()
        except Exception as e:
                return (False, postgres_db_conn_str,  e)
        return (True, postgres_db_conn_str,  None)

    def csv_to_postgres_append(csv_file_path: str, db_connection_string: str, mapping: dict, database_table: str, schema:str):
        # Read the CSV file into a Pandas DataFrame
        csv_file = csv_file_path
        df = pd.read_csv(csv_file)
        # Remove empty rows
        df.dropna(axis=0, how='all', inplace=True)
        # Display the columns in the CSV file
        print("Columns in CSV file:", df.columns)
        db_connection = db_connection_string
        engine = create_engine(db_connection)
        # Define mapping between CSV columns and database columns
        column_mapping = mapping

        # disbursements done
        # # Add 'id' column starting from 7001
        # starting_id = 7000
        # df['id'] = range(starting_id, starting_id + len(df))

        # Rename columns in the DataFrame to match the database table columns
        df.rename(columns=column_mapping, inplace=True)
        try:
            print('Migration starting...')
            # Push the data from the DataFrame to the database table
            df.to_sql(database_table, con=engine, if_exists='append', index=False, schema=schema)
            print('Excel to DB Migration was successful')
        except Exception as e:
            print(f'Excel to DB Migration was not successful. Migration failed with error:{e}')