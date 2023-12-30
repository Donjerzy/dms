from utils.csv_postgres import csv_to_postgres
from sqlalchemy import create_engine


if __name__ == '__main__':
    print('Welcome')
    print('Choose Migration Option:')
    print(' 1. CSV to Postgres')
    try:
        migration_option = int(input('Option: '))
    except ValueError:
        print('Invalid Migration Option')    
        quit()

    match migration_option:
        case 1:
            csv_path = ''
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
            print('Enter the file path')
            incorrect_path = True
            while incorrect_path:
                print('Paste in/ Enter the path to the csv file')
                print('The format should be like: C:\\\\file.csv')
                csv_path: str = input('Paste in/ Enter the path to the csv file: ')
                if csv_to_postgres.is_valid_csv(csv_path.strip()):
                    incorrect_path = False
                    break
                else:
                    print(f'{csv_path} is an invalid path')
            incorrect_connection_string = True
            while incorrect_connection_string:
                db_host = input('Enter the database host: ')
                db_username = input('Enter the database username: ')
                db_password = input('Enter the database password: ')
                db_name = input('Enter the database name: ')
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
            database_table = input('Enter the name of the database table: ')
            schema:str = input('Enter the target schema, if nothing is entered public will be the default schema: ')
            csv_to_postgres.csv_to_postgres_append(
                csv_file_path = csv_path.strip(),
                db_connection_string = connection_string,
                mapping = excel_db_column_mapping,
                database_table = database_table ,
                schema = schema if len(schema) >= 1 and schema.strip() != '' else 'public' 
            )
        case _:
            print("Yaay, I didn't have to do anything")	







# table_name = 'your_table_name'

# for index, row in df_excel.iterrows():
#     employer = row['Employer']
#     sector = row['Sector']

#     # Execute an UPDATE statement for each row
#     with db_connection.cursor() as cursor:
#         update_query = f"UPDATE {table_name} SET Sector = {sector} WHERE Employer = '{employer}'"
#         cursor.execute(update_query)

# # Commit the changes
# db_connection.commit()


