from google.cloud import bigquery
from google.oauth2 import service_account
import matplotlib.pyplot as plt
import numpy as np

credentials = service_account.Credentials.from_service_account_file('./durable-height-412117-2cfee8131ab0.json')

project_id = 'durable-height-412117'
client = bigquery.Client(credentials= credentials,project=project_id)

def search_country_data():
    """Search Country inflation rate with three options: Country Code, Name, IMF Code"""

    while True:
        print("+-----------------------------+")
        print("|  Search A Country Data:     |")
        print("|  1. Search by Country Code  |")
        print("|  2. Search by Country Name  |")
        print("|  3. Search by IMF Code      |")
        print("|  q. Quit                    |")
        print("+-----------------------------+")

        option = input("Choose an option: ").lower()

        if option == 'q':
            print("Exiting search.")
            break

        if option in ['1', '2', '3']:
            identifier = ""
            type_inflation = ""
            start_year = ""
            end_year =""

            if option == '1':
                while True:
                    identifier = input("Enter Country Code (3 Alphabetical Letters): ").upper()
                    if len(identifier) == 3 and identifier.isalpha():
                        break
                    else:
                        print("Invalid! Country Code has to be 3 Alphabet letters! Try agian!")
            elif option == '2':
                identifier = input("Enter Country Name: ").title()
            elif option == '3':
                while True:
                    identifier = input("Enter IMF Code (3 digits): ")
                    if identifier.isdigit() and 100 <= int(identifier) <=999:
                        break
                    else:
                        print("Invaild! IMF Country Code has to be 3 digits!")
            type_inflation = input("Choose Inflation Type:\n1. Energy Consumer Price Inflation\n2. Food Consumer Price Inflation\n3. Headline Consumer Price Inflation\n4. Producer Price Inflation \nEnter your choice: ")

            if type_inflation not in ['1', '2', '3', '4']:
                print("Invalid option")
                continue

            start_year = input("Enter the start year: ")
            end_year = input("Enter the end year: ")
            if not start_year.isdigit() or not end_year.isdigit():
                print("Invalid input. Please enter valid numeric years.")
                return
            table_mapping = {
                '1': 'Energy',
                '2': 'Food',
                '3': 'Headline',
                '4': 'Producer'
            }

            table_name = f'durable-height-412117.global_inflation.{table_mapping[type_inflation]}'

            placeholders = ', '.join([f"`_{year}`" for year in range(int(start_year), int(end_year) + 1)])
            
            if option =='1':
                query = f"SELECT `{table_name}`.`Country_Code`, {placeholders} " \
                        f"FROM `{table_name}` " \
                        f"WHERE `{table_name}`.`Country_Code` = @identifier"

            if option == '2':
                query = f"SELECT `{table_name}`.`Country`, {placeholders} " \
                        f"FROM `{table_name}` " \
                        f"WHERE `{table_name}`.`Country` = @identifier"
            elif option == '3':
                query = f"SELECT `{table_name}`.`IMF_Country_Code`, {placeholders} " \
                        f"FROM `{table_name}` " \
                        f"WHERE `{table_name}`.`IMF_Country_Code` = @identifier"

            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("identifier", "STRING", identifier),
                ]
            )
            query_job = client.query(query, job_config=job_config)
            data = list(query_job)

            if data:
                print("Search Results:")
                for row in data:
                    print(row.values())

                # Plot the data using Matplotlib
                years = [f'_{year}' for year in range(int(start_year), int(end_year) + 1)]
                values = [row[year] for year in years]
                plt.plot(years, values, label=f"{identifier} - {type_inflation}")

                plt.xlabel("Years")
                plt.ylabel("Inflation Rate")
                plt.title("Inflation Rate Over Time")
                plt.legend()
                plt.show()

            else:
                print(f"No data found wiht {identifier}.")

        else:
            print("Invalid option. Try Again.")
            continue

        search_again = input("Do you want to search for another country? (y/n): ").lower()
        if search_again != 'y':
            print("Exiting search.")
            break


def country_highest_lowest_rate():
    """Find a country's highest and lowest year of inflation rate"""

    while True:
        print("+------------------------------------------------------------+")
        print("|  Search Country's Highest/Lowest Inflation Rate:           |")
        print("+------------------------------------------------------------+")
        identifier = input("Enter the country name for which you want to query: ").title()
        start_year = input("Enter the start year: ")
        end_year = input("Enter the end year: ")

        table_names = ['Energy', 'Food', 'Headline', 'Producer']
        highest_values = {}
        lowest_values = {}

        for table_name in table_names:
            placeholders = ', '.join([f"`_{year}`" for year in range(int(start_year), int(end_year) + 1)])
            
            # Query for the highest rate
            query_highest = f'''
                SELECT "Country", {placeholders}
                FROM `durable-height-412117.global_inflation.{table_name}`
                WHERE Country = @identifier
                ORDER BY {placeholders} DESC
                LIMIT 1
            '''

            # Query for the lowest rate
            query_lowest = f'''
                SELECT "Country", {placeholders}
                FROM `durable-height-412117.global_inflation.{table_name}`
                WHERE Country = @identifier
                ORDER BY {placeholders} ASC
                LIMIT 1
            '''

            params = [bigquery.ScalarQueryParameter("identifier", "STRING", identifier)]
            job_config = bigquery.QueryJobConfig(query_parameters=params)

            query_job_highest = client.query(query_highest, job_config=job_config)
            rows_highest = list(query_job_highest)

            if rows_highest:
                highest_values[table_name] = rows_highest[0]

            query_job_lowest = client.query(query_lowest, job_config=job_config)
            rows_lowest = list(query_job_lowest)

            if rows_lowest:
                lowest_values[table_name] = rows_lowest[0]

        print("\nHighest Inflation Rates:")
        for table_name, highest in highest_values.items():
            valid_values = []
            max_year=None
            for key, value in highest.items():
                if key != 'Country' and value is not None:
                    try:
                        valid_values.append(float(value))
                    except ValueError:
                        continue

            highest_rate = max(valid_values) if any(valid_values) else None
            print(f"{table_name}: {identifier} - Highest rate: {highest_rate}")

        print("\nLowest Inflation Rates:")
        for table_name, lowest in lowest_values.items():
            valid_values = []
            for key, value in lowest.items():
                if key != 'Country' and value is not None:
                    try:
                        valid_values.append(float(value))
                    except ValueError:
                        continue
            lowest_rate = min(valid_values) if any(valid_values) else None
            print(f"{table_name}: {identifier} - Lowest rate: {lowest_rate}")

        # Visualize the data using Matplotlib
        visualize_country_data(identifier, highest_values, lowest_values, start_year, end_year)

        another_country = input("Do you want to see inflation rates for another country? (y/n): ").lower()
        if another_country != 'y':
            print("Exiting search for country's highest/lowest inflation rate.")
            break

def visualize_country_data(identifier, highest_values, lowest_values, start_year, end_year):
    """Visualize the highest and lowest inflation rates using Matplotlib"""

    # Extract highest and lowest rates for each type within the specified range of years
    highest_rates = {table: {'year': None, 'rate': None, 'color': None} for table in highest_values.keys()}
    lowest_rates = {table: {'year': None, 'rate': None, 'color': None} for table in lowest_values.keys()}

    # Assign 8 different colors
    colors = ['blue', 'green', 'red', 'purple', 'orange', 'cyan', 'pink', 'brown']

    for i, (table, data) in enumerate(highest_rates.items()):
        data['color'] = colors[i]

    for i, (table, data) in enumerate(lowest_rates.items()):
        data['color'] = colors[i]

    for table, highest in highest_values.items():
        years = [f'_{year}' for year in range(int(start_year), int(end_year) + 1)]
        rates = [highest[key] if key in highest.keys() and highest[key] is not None else None for key in years]

        if any(rates) and None not in rates:
            max_rate = max(rates)
            max_year = years[rates.index(max_rate)]
            highest_rates[table]['year'] = int(max_year[1:])
            highest_rates[table]['rate'] = max_rate

    for table, lowest in lowest_values.items():
        years = [f'_{year}' for year in range(int(start_year), int(end_year) + 1)]
        rates = [lowest[key] if key in lowest.keys() and lowest[key] is not None else None for key in years]

        if any(rates) and None not in rates:
            min_rate = min(rates)
            min_year = years[rates.index(min_rate)]
            lowest_rates[table]['year'] = int(min_year[1:])
            lowest_rates[table]['rate'] = min_rate

    # Bar chart for highest and lowest rates
    plt.figure(figsize=(14, 6))

    bar_width = 0.35
    index = np.arange(len(highest_rates) * 2)

    for i, (table, data) in enumerate(highest_rates.items()):
        if data['rate'] is not None:
            plt.bar(index[i], data['rate'], bar_width, label=f"{table} - Highest", alpha=0.7, color=data['color'])
            plt.text(index[i], data['rate'], f"{data['rate']:.2f}\n{data['year']}", ha='center', va='bottom', color='black')

    for i, (table, data) in enumerate(lowest_rates.items()):
        if data['rate'] is not None:
            plt.bar(index[i + len(highest_rates)], data['rate'], bar_width, label=f"{table} - Lowest", alpha=0.7, color=data['color'])
            plt.text(index[i + len(highest_rates)], data['rate'], f"{data['rate']:.2f}\n{data['year']}", ha='center', va='bottom', color='black')

    plt.xlabel("Inflation Types")
    plt.ylabel("Inflation Rate")
    plt.title(f"Highest and Lowest Inflation Rates for {identifier} ({start_year}-{end_year})")
    plt.xticks(index + bar_width / 2, [f"{table} - Highest" for table in highest_rates.keys()] + [f"{table} - Lowest" for table in lowest_rates.keys()])
    plt.legend()
    plt.show()


def compare_countries():
    """Compare inflation rates of multiple countries for a specific year and type."""
    print("+------------------------------------------------------------------------------+")
    print("|  Enter countries name to compare with (or type 'Done' to start comparison):  |")
    print("+------------------------------------------------------------------------------+")
    while True:
        countries_to_compare = []

        # Ask user for countries until they enter 'Done'
        while True:
            country = input(" ").title()
            if country == 'Done':
                break
            else:
                countries_to_compare.append(country)

        # Check if the user entered at least one country
        if not countries_to_compare:
            print("No countries entered. Exiting comparison.")
            break  # Exit the entire comparison process

        while True:
            print("Select Type of Inflation to Compare:")
            print("1. Energy Consumer Price Inflation")
            print("2. Food Consumer Price Inflation")
            print("3. Headline Consumer Price Inflation")
            print("4. Producer Price Inflation")
            print("q. Quit")

            option = input("Choose which table to compare: ").lower()
            if option == 'q':
                print("Exiting country comparison.")
                return  # Exit the function and stop the comparison
            elif option not in ['1', '2', '3', '4']:
                print("Invalid option. Please choose a valid option.")
                continue

            table_mapping = {
                '1': 'Energy',
                '2': 'Food',
                '3': 'Headline',
                '4': 'Producer'
            }

            table_name = f'durable-height-412117.global_inflation.{table_mapping[option]}'
            while True:
                year = input("Enter the year(1970-2022) for which you want to compare inflation rates: ")
                if not year.isdigit() or len(year) != 4:
                    print("Plear enter a valid year! ")
                    continue
                
                break
            highest_rate = 0
            lowest_rate = float('inf')
            highest_country = ""
            lowest_country = ""

            # Lists to store data for visualization
            countries_labels = []
            inflation_rates = []

            for country in countries_to_compare:
                country = country.title()
                year_column = f"_{year}"
                # Retrieve inflation rates for selected countries in the specified year
                query_rates = f'''
                    SELECT {year_column} AS InflationRate
                    FROM `{table_name}`
                    WHERE Country = @country
                '''
                params = {"country": country}
                query_job_rates = client.query(
                    query_rates,
                    job_config=bigquery.QueryJobConfig(
                        query_parameters=[
                            bigquery.ScalarQueryParameter("country", "STRING", params["country"]),
                        ]
                    )
                )

                rates = list(query_job_rates)

                if rates:
                    rate = rates[0]['InflationRate']
                    print(f"{country} - Inflation Rate for {year}: {rate}")

                    # Update highest and lowest rates and corresponding countries
                    if rate is not None and rate > highest_rate:
                        highest_rate = rate
                        highest_country = country

                    if rate is not None and rate < lowest_rate:
                        lowest_rate = rate
                        lowest_country = country

                    # Add data for visualization
                    countries_labels.append(country)
                    inflation_rates.append(rate)
                else:
                    print(f"No data found for {country} in {table_name} for {year}.")

            # Display the results
            print("\nComparison Results:")
            print(f"Highest Inflation Rate: {highest_country} - {highest_rate}")
            print(f"Lowest Inflation Rate: {lowest_country} - {lowest_rate}")

            visualize_comparison_data(countries_labels, inflation_rates, year)

            another_comparison = input("Do you want to compare more countries? (y/n): ").lower()
            if another_comparison != 'y':
                print("Exiting country comparison.")
                return  # Exit the function and stop the comparison

def visualize_comparison_data(countries, inflation_rates, year):
    """Visualize the inflation rates for multiple countries using Matplotlib."""

    # Filter out None values
    valid_data = [(country, rate) for country, rate in zip(countries, inflation_rates) if rate is not None]
    if not valid_data:
        print("No valid data to visualize.")
        return
    
    countries, inflation_rates = zip(*valid_data)
    colors = plt.cm.viridis(np.linspace(0, 1, len(countries)))

    plt.bar(countries, inflation_rates, color=colors)
    plt.xlabel('Countries')
    plt.ylabel('Inflation Rate')
    plt.title(f"Inflation Rates for Multiple Countries in {year}")
    plt.show()