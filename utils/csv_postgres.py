import pandas as pd
from sqlalchemy import create_engine


class csv_to_postgres:

    def csv_to_postgres_append(csv_file_path: str, db_connection_string: str):
        # Read the CSV file into a Pandas DataFrame
        csv_file = csv_file_path
        df = pd.read_csv(csv_file)

        # Remove empty rows
        df.dropna(axis=0, how='all', inplace=True)

        # Display the columns in the CSV file
        print("Columns in CSV file:", df.columns)

        # Display file contents
        # print("File Contents:", df)

        # Define your database connection
        
        db_connection = (db_connection_string)

        engine = create_engine(db_connection)

        # Try connecting to the database
        try:
            # Use the engine to establish a connection
            connection = engine.connect()

            # If the connection is successful, print a success message
            print("Connection successful!")

            # Remember to close the connection
            connection.close()
        except Exception as e:
            print(f"Connection failed with error: {e}")


        # Define mapping between CSV columns and database columns
        column_mapping = {
            'created': 'created',
            'name': 'name',
            'code': 'code',
            'location_type': 'location_type',
            'city_id': 'city_id'
        }



        # disbursements done
        # # Add 'id' column starting from 7001
        # starting_id = 7000
        # df['id'] = range(starting_id, starting_id + len(df))

        # Rename columns in the DataFrame to match the database table columns
        df.rename(columns=column_mapping, inplace=True)

        try:
            print('Migration starting...')
            # Push the data from the DataFrame to the database table
            df.to_sql('loans_location', con=engine, if_exists='append', index=False, schema='fin_kenya')
            print('Excel to DB Migration was successful')
        except Exception as e:
            print(f'Excel to DB Migration was not successful. Migration failed with error:{e}')