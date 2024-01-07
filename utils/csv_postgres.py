import pandas as pd
from sqlalchemy import create_engine, Column, String, Integer, update
from sqlalchemy.orm import DeclarativeBase


class csv_to_postgres:

    # TODO: Self to initialize DB Connection String
    # def __init__(self):
    #     pass

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
                = csv_to_postgres.test_connection_to_postgres_db(
                {'host': f'{db_host}', 'u_name': f'{db_username}', 'pass': f'{db_password}', 'db_name': f'{db_name}',
                 'port': f'{db_port}'})
            if (is_valid_connection_string):
                incorrect_connection_string = False
                break
            else:
                print(f'The connection was not made due to error {connection_error_thrown}')
        return connection_string

    def is_valid_csv(self, csv_file_path):
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

    def test_connection_to_postgres_db(self, connection_dict: dict):
        postgres_db_conn_str = f'postgresql+psycopg2://{connection_dict["u_name"]}:{connection_dict["pass"]}@{connection_dict["host"]}:{connection_dict["port"]}/{connection_dict["db_name"]}'
        engine = create_engine(postgres_db_conn_str)
        try:
            connection = engine.connect()
            connection.close()
        except Exception as e:
            return (False, postgres_db_conn_str, e)
        return (True, postgres_db_conn_str, None)

    def csv_to_postgres_append(self, csv_file_path: str, db_connection_string: str, mapping: dict, database_table: str,
                               schema: str):
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
        columns_dict = {}
        primary_key_found = False
        primary_key = ''
        in_progress = True
        print('You will be prompted to enter columns and their data types')
        while in_progress:
            print('Current supported data types are integer and string. Enter i for integer and s for string')
            column_name = input('Enter column name: ')
            column_data_type = input('Enter column datatype. i for integer or s for string')
            column_data_type_transformed = String if column_data_type == 's' else Integer
            if not primary_key_found:
                primary_key = input('Is this column a primary key? Enter y for yes anything else for no ')
                primary_key = column_name if primary_key == 'y' else ''
                primary_key_found = True
            valid_column = (f'Are you happy with column: {column_name}: {column_data_type_transformed}?'
                            f'enter y for yes anything else for no: ')
            if valid_column.strip() == 'Y' or valid_column.strip() == 'y':
                columns_dict[column_name] = column_data_type_transformed
            else:
                print('The column entered will be disregarded')
            user_done = input('Are you done? Enter y for yes and anything else for no: ')
            if (user_done == 'y' and primary_key_found) or (user_done == 'Y' and primary_key_found):
                in_progress = False
            elif (user_done == 'y' and not primary_key_found) or (user_done == 'Y' and not primary_key_found):
                print('You did not provide a primary key')
                print('The process is starting again...')
                columns_dict = {}
        return columns_dict, primary_key

    def create_dynamic_class(self, table_name: str, columns: dict, schema, primary_key):
        attributes = {'__tablename__': table_name}

        # Define the columns dynamically
        for column_name, column_type in columns.items():
            attributes[column_name] = Column(column_type, primary_key=True if primary_key == column_name else False)

        # Set schema if provided
        if schema:
            attributes['__table_args__'] = {'schema': schema}

        # Create the class using type
        dynamic_class = type(table_name.capitalize(), (DeclarativeBase,), attributes)

        return dynamic_class

    # Untested
    def csv_to_postgres_update_one_column(self, csv_file_path: str, db_connection_string: str, database_table: str,
                                          schema: str):
        engine = create_engine(db_connection_string)
        columns, primary_key = self.get_table_model()
        TableModel = self.create_dynamic_class(
            table_name=database_table,
            columns=columns,
            schema=schema,
            primary_key=primary_key
        )
        df = pd.read_csv(csv_file_path)
        column_to_be_searched = df.columns[0]
        column_to_update = df.columns[1]
        update_dict = {}
        for index, row in df.iterrows():
            to_be_updated = row[column_to_be_searched]
            update_value = row[column_to_update]
            update_dict[to_be_updated] = update_value
        print(f'Number of values to be updated = {len(update_dict.keys())}')
        count = 0
        with engine.connect() as conn:
            for search, value in update_dict.items():
                count += 1
                print(f'On record {count}')
                try:
                    conn.execute(
                        update(TableModel).where(TableModel.column_to_be_searched == search)
                        .values(column_to_update= value))
                    conn.commit()
                except Exception as e:
                    print(f'Record {column_to_be_searched} {search} could not be updated due to error {e}')
        print('Update is done')
