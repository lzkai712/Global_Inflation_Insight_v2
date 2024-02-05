from google.cloud import bigquery
from google.oauth2 import service_account

credentials = service_account.Credentials.from_service_account_file('./durable-height-412117-2cfee8131ab0.json')

project_id = 'durable-height-412117'
client = bigquery.Client(credentials= credentials,project=project_id)

from google.cloud import bigquery
from google.oauth2 import service_account

credentials = service_account.Credentials.from_service_account_file('./durable-height-412117-2cfee8131ab0.json')

project_id = 'durable-height-412117'
client = bigquery.Client(credentials=credentials, project=project_id)

def manage_add_data():
    while True:
        print("+------------------------------------------+")
        print("|    Add New Inflation Data:               |")
        print("|    1. Energy Consumer Price Inflation    |")
        print("|    2. Food Consumer Price Inflation      |")
        print("|    3. Headline Consumer Price Inflation  |")
        print("|    4. Producer Price Inflation           |")
        print("|    q. Quit                               |")
        print("+------------------------------------------+")

        option = input("Choose which table to add new data: ").lower()
        if option == 'q':
            print("Exiting add data.")
            break

        table_mapping = {
            '1': 'Energy',
            '2': 'Food',
            '3': 'Headline',
            '4': 'Producer'
        }

        table_name = f'durable-height-412117.global_inflation.{table_mapping[option]}'
        if table_name:
            add_type = input("Choose an option:\n1. Add new data for existing country\n2. Add a new country to the table: \nEnter your choice: ")
            if add_type == '1':
                identifier = input("Enter the country name for which you want to add data: ").title()

                # Construct the query to check if the country exists in the "Country" column
                query = f'SELECT * FROM `{table_name}` WHERE Country = @identifier '
                
                params = [bigquery.ScalarQueryParameter("identifier", "STRING", identifier)]
                job_config = bigquery.QueryJobConfig(query_parameters=params)
                query_job = client.query(query, job_config=job_config)
                existing_data = list(query_job)

                if existing_data:
                    while True:
                        new_column_name = input("Enter the new column name (e.g., '_2023'): ")
                        if new_column_name.startswith('_') and len(new_column_name) == 5 and new_column_name[1:].isdigit():
                            break
                        else:
                            print("Invalid format. Please follow this format '_xxxx' for year: ")
                    
                    # Retrieve the table schema
                    table_ref = client.dataset('global_inflation').table(table_mapping[option])
                    table = client.get_table(table_ref)

                    # Check if the column already exists in the schema
                    existing_columns = [field.name for field in table.schema]

                    if new_column_name not in existing_columns:
                        # Add a new column to the schema
                        new_field = bigquery.SchemaField(new_column_name, 'FLOAT')
                        table.schema = table.schema + [new_field]
                        client.update_table(table, ['schema'])
                    new_value = float(input(f"Enter the value for {new_column_name}: "))
                    # Update the existing data with the new value using parameterized query
                    query_update_data = f'''
                        UPDATE `{table_name}`
                        SET `{new_column_name}` = @new_value
                        WHERE Country = @identifier
                    '''
                    params_update_data = [
                        bigquery.ScalarQueryParameter("new_value", "FLOAT", new_value),
                        bigquery.ScalarQueryParameter("identifier", "STRING", identifier),
                    ]

                    job_config_update_data = bigquery.QueryJobConfig(query_parameters=params_update_data)
                    client.query(query_update_data, job_config=job_config_update_data).result()
                    print("New data added successfully!")
                else:
                    print(f"The specified country '{identifier}' does not exist in the table. Please choose a valid country.")


            elif add_type == '2':
                # Add new row for a new country
                while True:
                    country_code = input("Enter Country Code: ").upper()
                    if len(country_code) == 3 and country_code.isalpha():
                        break
                    else:
                        print("Invalid! Country Code has to be 3 Alphabet letters! Try agian!")
                while True:
                    imf_country_code = input("Enter IMF Country Code: ")
                    if len(imf_country_code) ==3 and imf_country_code.isdigit():
                        break
                    else:
                        print("Invalid! IMF Country Code has to be 3 Digits! Try agian!")
                country_name = input("Enter Country: ").title()

                insert_query = f'INSERT INTO `{table_name}` (Country_Code, IMF_Country_Code, Country, Indicator_Type, Series_Name) VALUES (@country_code, @imf_country_code, @country_name, @indicator_type, @series_name)'
                query_params = [
                    bigquery.ScalarQueryParameter("country_code", "STRING", country_code),
                    bigquery.ScalarQueryParameter("imf_country_code", "INTEGER", imf_country_code),
                    bigquery.ScalarQueryParameter("country_name", "STRING", country_name),
                ]

                job_config_insert = bigquery.QueryJobConfig(query_parameters=query_params)
                client.query(insert_query, job_config=job_config_insert).result()
                print("New row added successfully!")

            else:
                print("Invalid option. Please try again.")
        else:
            print("Invalid option. Please try again.")

def manage_update_data():
    while True:
        print("+---------------------------------------+")
        print("|  Update Inflation Data:               |")
        print("|  1. Energy Consumer Price Inflation   |")
        print("|  2. Food Consumer Price Inflation     |")
        print("|  3. Headline Consumer Price Inflation |")
        print("|  4. Producer Price Inflation          |")
        print("|  q. Quit                              |")
        print("+---------------------------------------+")

        option = input("Choose an option: ").lower()
        if option == 'q':
            print("Exiting update data.")
            break

        table_mapping = {
            '1': 'Energy',
            '2': 'Food',
            '3': 'Headline',
            '4': 'Producer'
        }

        table_name = f'durable-height-412117.global_inflation.{table_mapping[option]}'
        if table_name:
            identifier = input("Enter the country name for which you want to update data: ").title()

            # check if the country exists in the "Country" column
            query = f'SELECT * FROM `{table_name}` WHERE Country = @identifier'
                
            params = [bigquery.ScalarQueryParameter("identifier", "STRING", identifier)]
            job_config = bigquery.QueryJobConfig(query_parameters=params)
            query_job = client.query(query, job_config=job_config)
            existing_data = list(query_job)

            if existing_data:
                while True:
                    column_name = input("Enter the new column name (e.g., '_2023'): ")
                    if column_name.startswith('_') and len(column_name) == 5 and column_name[1:].isdigit():
                        break
                    else:
                        print("Invalid format. Please follow this format '_xxxx' for year: ")
                new_value = float(input("Enter the new value: "))

                query_update_data = f'UPDATE `{table_name}` SET `{column_name}` = @new_value WHERE Country = @identifier'
                params_update_data = [
                    bigquery.ScalarQueryParameter("new_value", "FLOAT", new_value),
                    bigquery.ScalarQueryParameter("identifier", "STRING", identifier)
                ]
                job_config_update_data = bigquery.QueryJobConfig(query_parameters=params_update_data)

                client.query(query_update_data, job_config=job_config_update_data).result()
                print("Data updated successfully!")
            else:
                print(f"The specified country '{identifier}' does not exist in the table. Please choose a valid country.")
        else:
            print("Invalid option. Please try again.")

def manage_delete_data():
    while True:
        print("+----------------------------------------+")
        print("|  Which Inflation Data to Delete ?:     |")
        print("|  1. Energy Consumer Price Inflation    |")
        print("|  2. Food Consumer Price Inflation      |")
        print("|  3. Headline Consumer Price Inflation  |")
        print("|  4. Producer Price Inflation           |")
        print("|  q. Quit                               |")
        print("+----------------------------------------+")
        option = input("Choose an option: ").lower()
        if option == 'q':
            print("Exiting delete data.")
            break

        table_mapping = {
            '1': 'Energy',
            '2': 'Food',
            '3': 'Headline',
            '4': 'Producer'
        }

        table_name = f'durable-height-412117.global_inflation.{table_mapping[option]}'
        if table_name:
            delete_option = input("Choose an option for deletion:\n1. Delete A Country's data(row)\n2. Delete entire column\n3. Delete specific value\nWhat would you like to do: ")

            if delete_option == '1':
                identifier = input("Enter the country name for which you want to delete data: ").title()

                query_delete_data = f'DELETE FROM `{table_name}` WHERE Country = "{identifier}"'
                client.query(query_delete_data).result()
                print("Row deleted successfully!")

            elif delete_option == '2':
                while True:
                    column_name = input("Enter the column of year e.g., '_2023' to delete: ")
                    if column_name.startswith('_') and len(column_name) == 5 and column_name[1:].isdigit():
                        break
                    else:
                        print("Invalid format. Please follow this format '_xxxx' for year: ")

                query_delete_column = f'ALTER TABLE `{table_name}` DROP COLUMN {column_name}'
                client.query(query_delete_column).result()
                print("Column deleted successfully!")

            elif delete_option == '3':
                column_name = input("Enter the column(year ex:_2000) for which you want to delete a specific value: ")
                identifier = input("Enter the country name for which you want to delete a specific value: ").lower()

                query_delete_value = f'UPDATE `{table_name}` SET `{column_name}`= NULL WHERE Country = "{identifier}"'
                params_delete_value = [
                    bigquery.ScalarQueryParameter("identifier", "STRING", identifier)
                ]
                job_config_delete_value = bigquery.QueryJobConfig(query_parameters=params_delete_value)
                client.query(query_delete_value, job_config=job_config_delete_value).result()
                print("Value deleted successfully!")

            else:
                print("Invalid option for deletion. Please try again.")

        else:
            print("Invalid option. Please try again.")
