from utils.csv_postgres import csv_to_postgres

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


