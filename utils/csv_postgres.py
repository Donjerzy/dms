import pandas as pd
from sqlalchemy import create_engine, Column
from sqlalchemy.orm import DeclarativeBase


class csv_to_postgres:

    def get_csv_file_path(self):
        print('Enter the file path')
        incorrect_path = True
        csv_path = ''
        while incorrect_path:
            print('Paste in/ Enter the path to the csv file')
            print('The format should be like: C:\\\\file.csv')
            csv_path = input('Paste in/ Enter the path to the csv file: ')
            if self.is_valid_csv(csv_path.strip()):
                incorrect_path = False
                break
            else:
                print(f'{csv_path} is an invalid path')
        return csv_path
    

    def get_db_connection_string(self):
        incorrect_connection_string = True
        while incorrect_connection_string:
            db_host = input('Enter the database host: ')
            db_username = input('Enter the database username: ')
            db_password = input('Enter the database password: ')
            db_name = input('Enter the database name: ')
            db_port = 5432
            invalid_port = True
            while invalid_port:
                try:
                    db_port = int(input('Enter the database port: '))
                    invalid_port = False
                    break
                except ValueError:
                    print('The port number entered is invalid')     
            is_valid_connection_string, connection_string, connection_error_thrown \
                = csv_to_postgres.test_connection_to_postgres_db({ 'host': f'{db_host}', 'u_name': f'{db_username}', 'pass': f'{db_password}','db_name': f'{db_name}', 'port': f'{db_port}'})
            if (is_valid_connection_string):
                incorrect_connection_string = False
                break
            else:
                print(f'The connection was not made due to error {connection_error_thrown}')
        return connection_string

    def is_valid_csv(self,csv_file_path):
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

    
    def get_table_model(self):
        in_progress = True
        while in_progress:
            print('You will be prompted to enter columns and their data types')
            print('Current supported data types are integres and string. Enter i for integer and s for string')
            




        return {}



    def create_dynamic_class(self, table_name:str, columns:{}, schema):
        attributes = {'__tablename__': table_name}

        # Define the columns dynamically
        for column_name, column_type in columns.items():
            attributes[column_name] = Column(column_type)

        # Set schema if provided
        if schema:
            attributes['__table_args__'] = {'schema': schema}

        # Create the class using type
        dynamic_class = type(table_name.capitalize(), (DeclarativeBase,), attributes)

        return dynamic_class
    

    def csv_to_postgres_update_one_column(self, csv_file_path:str, db_connection_string:str, database_table:str, schema:str):
        TableModel = self.create_dynamic_class(
            table_name=database_table,
            columns=self.get_table_model(),
            schema=schema    
        )