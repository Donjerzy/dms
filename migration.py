import pandas as pd
from sqlalchemy import create_engine

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



if __name__ == '__main__':
    print('Welcome')
    print('Choose Migration Option:')
    print(' 1. Excel to Postgres')
    try:
        migration_option = int(input('Option: '))
    except ValueError:
        print('Invalid Migration Option')    
        quit()

    match migration_option:
        case 1:
            column_mapping_in_progress: False
            excel_db_column_mapping: dict = {}
            print("Enter the columns in your excel sheet with the columns in the db they should map to:")
            column_mapping_in_progress = True
            while column_mapping_in_progress:
                sheet_column:str = input('Excel Sheet Column Name: ')
                db_column:str = input(f'Postgres Table Column Name for {sheet_column} = ')
                print(f'Are you happy with {sheet_column} - {db_column} mapping?')
                happy:str = input('Enter Y anything else for no ')
                if happy.lower() == 'y':
                    excel_db_column_mapping[excel_db_column_mapping.get(sheet_column, sheet_column)] = db_column
                else:
                    print('Values entered have been disregarded')
                print('Do you want to continue column mapping?')
                continue_mapping: str = input('Enter Y anything else for no ')
                column_mapping_in_progress = True if continue_mapping.lower() == 'y' else False
            print('The final mapping is:',excel_db_column_mapping)
            quit()    
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


