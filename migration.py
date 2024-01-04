from utils.csv_postgres import csv_to_postgres
from sqlalchemy import create_engine


if __name__ == '__main__':
    print('Welcome')
    print('Choose Migration Option:')
    print(' 1. CSV to Postgres')
    print(' 2. CSV to Postgres Update One Specific Column Values')
    csv_path = ''
    try:
        migration_option = int(input('Option: '))
    except ValueError:
        print('Invalid Migration Option')    
        quit()
    match migration_option:
        case 1:
            csv_postgres_util = csv_to_postgres()  
            column_mapping_in_progress: False
            excel_db_column_mapping: dict = {}
            print("Enter the columns in your excel sheet with the columns in the db they should map to:")
            column_mapping_in_progress = True
            while column_mapping_in_progress:
                sheet_column:str = input('Excel Sheet Column Name: ')
                db_column:str = input(f'Postgres Table Column Name for {sheet_column}: ')
                print(f'Are you happy with {sheet_column} - {db_column} mapping?')
                happy:str = input('Enter Y for yes anything else for no: ')
                if happy.lower() == 'y':
                    excel_db_column_mapping[sheet_column] = db_column
                else:
                    print('Values entered have been disregarded')
                print('Do you want to continue column mapping?')
                continue_mapping: str = input('Enter Y for yes anything else for no: ')
                column_mapping_in_progress = True if continue_mapping.lower() == 'y' else False
            print('The final mapping is:', excel_db_column_mapping)
            csv_path = csv_postgres_util.get_csv_file_path()
            connection_string = csv_postgres_util.get_db_connection_string()
            database_table = input('Enter the name of the database table: ')
            schema:str = input('Enter the target schema, if nothing is entered public will be the default schema: ')
            csv_postgres_util.csv_to_postgres_append(
                csv_file_path = csv_path.strip(),
                db_connection_string = connection_string,
                mapping = excel_db_column_mapping,
                database_table = database_table ,
                schema = schema if len(schema) >= 1 and schema.strip() != '' else 'public' 
            )
        case 2:
            csv_postgres_util = csv_to_postgres()
            print('Make sure the csv file is formatted as follows:')
            print('Header(Column to be updated) | Header(Update Value Header)')
            print('Ensure search value is the left colum and update value is the second column')
            csv_path = csv_postgres_util.get_csv_file_path().strip()
            connection_string = csv_postgres_util.get_db_connection_string()
            print(f'The connection string is {connection_string}')
            database_table = input('Enter the name of the database table: ')
            schema:str = input('Enter the target schema, if nothing is entered public will be the default schema: ')
            csv_postgres_util.csv_to_postgres_update_one_column(
                db_connection_string=connection_string,
                csv_file_path=csv_path,
                database_table=database_table,
                schema=schema if len(schema) >= 1 and schema.strip() != '' else 'public' 
            )
        case _:
            print("Yaay, I didn't have to do anything")	
