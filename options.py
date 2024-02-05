from data_analysis import search_country_data, country_highest_lowest_rate, compare_countries
from data_management import manage_add_data, manage_update_data, manage_delete_data

def is_valid_option(option):
    return option in ['1', '2', '3', '4','5']
def regular_user_interface():
    while True:
        print("+-----------------------------------------------------------------------------------+")
        print("|                      Regular User Interface:                                      |")
        print("|   1. Search country's inflation data                                              |")
        print("|   2. Identify country's highest and lowest inflation rate during range of years   |")
        print("|   3. Compare inflation rates between different countries                          |")
        print("|   4. Logout                                                                       |")
        print("+-----------------------------------------------------------------------------------+")

        choice = input("Enter your choice: ")
        if not choice.isdigit() or not is_valid_option(choice):
            print("Invalid input. Please enter a valid option (1-5).")
            continue

        if choice == '1':
            search_country_data()
        elif choice == '2':
            country_highest_lowest_rate()
        elif choice == '3':
            compare_countries()
        elif choice == '4':
            print("Logged out.")
            break
        else:
            print("Invalid choice. Please try again.")


def manager_interface():
    while True:
        print("\nManager Interface:")
        print("1. Add New Inflation Data")
        print("2. Update Inflation Data")
        print("3. Delete Inflation Data")
        print("4. Search Data")
        print("5. Sign out")

        choice = input("Enter your choice: ")
        if not choice.isdigit() or not is_valid_option(choice):
            print("Invalid input. Please enter a valid option (1-5).")
            continue

        if choice == '1':
            manage_add_data()
        elif choice == '2':
            manage_update_data()
        elif choice == '3':
            manage_delete_data()
        elif choice == '4':
            search_country_data()
        elif choice == '5':
            print("Logged out.")
            break
        else:
            print("Invalid choice. Please try again.")