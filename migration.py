from utils.csv_postgres import csv_to_postgres


def is_valid_csv(path):
    if len(path) < 4:
        return False
    file_extension = path[-4:-1]
    if file_extension != '.csv':
        return False
    return False


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
                db_column:str = input(f'Postgres Table Column Name for {sheet_column} = ')
                print(f'Are you happy with {sheet_column} - {db_column} mapping?')
                happy:str = input('Enter Y anything else for no ')
                if happy.lower() == 'y':
                    excel_db_column_mapping[sheet_column] = db_column
                else:
                    print('Values entered have been disregarded')
                print('Do you want to continue column mapping?')
                continue_mapping: str = input('Enter Y anything else for no ')
                column_mapping_in_progress = True if continue_mapping.lower() == 'y' else False
            print('The final mapping is:', excel_db_column_mapping)
            print('Enter the file path')
            incorrect_path = True
            while incorrect_path:
                csv_path: str = input('Paste in/ Enter the path to the csv file')
                if is_valid_csv(csv_path):
                    incorrect_path = False
                    break
                else:
                    print(f'{csv_path} is an invalid path')
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


