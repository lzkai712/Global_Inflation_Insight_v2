from google.cloud import bigquery
from google.oauth2 import service_account
import matplotlib.pyplot as plt
import getpass
import re
import datetime

credentials = service_account.Credentials.from_service_account_file('./durable-height-412117-2cfee8131ab0.json')

project_id = 'durable-height-412117'
client = bigquery.Client(credentials= credentials,project=project_id)

MANAGER_ACCOUNT = {
    "username": "zhongkai670@revature.net",
    "password": "123456789",
    "name" : "Manager"
}

def is_manager(username):
    return username == MANAGER_ACCOUNT["username"]

def is_valid_email(email):
    return re.match(r'^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$', email)

def register():
    """User registration process"""
    while True:
        f_name = input("Enter your first name: ").title()
        while not (f_name.isalpha()):
            print("Invalid first name. Please enter again, the name should only contain alphabetical letters.")
            f_name = input("Enter your first name: ").title()

        l_name = input("Enter your Last name: ").title()
        while not (l_name.isalpha()):
            print("Invalid last name. Please enter again, the name should only contain alphabetical letters.")
            l_name = input("Enter your Last name: ").title()

        dob = input("Enter your birthday in mm/dd/yyyy: ")
        while not re.match(r'^\d{2}/\d{2}/\d{4}$', dob):
            print("Invalid date of birth format. Please enter in mm/dd/yyyy format.")
            dob = input("Enter your birthday in mm/dd/yyyy: ")
            dob_bigquery_format = datetime.datetime.strptime(dob, '%m/%d/%Y').strftime('%Y-%m-%d')

        location = input("Enter city of your location (Optional): ")
        username = input("Enter your email to create an account: ").lower()
        while not is_valid_email(username):
            print("Invalid email format. Please enter a valid email address.")
            username = input("Enter your email to create an account: ").lower()

        password = getpass.getpass("Enter your password: ")
        while len(password) < 9:
            print("Password must be at least 9 characters long.")
            password = getpass.getpass("Enter your password: ")

        # Check if the user already exists
        query = f'''
                SELECT *
                FROM `durable-height-412117.global_inflation.users`
                WHERE Username = @username
                '''
        job_config = bigquery.QueryJobConfig(
                    query_parameters=[
                        bigquery.ScalarQueryParameter("f_name", "STRING", f_name),
                        bigquery.ScalarQueryParameter("l_name", "STRING", l_name),
                        bigquery.ScalarQueryParameter("dob", "STRING", dob),
                        bigquery.ScalarQueryParameter("location", "STRING", location),
                        bigquery.ScalarQueryParameter("username", "STRING", username),
                        bigquery.ScalarQueryParameter("password", "STRING", password),
                    ]
                )
        query_job = client.query(query)

        try:
            query_job = client.query(query, job_config=job_config)
            existing_user = list(query_job)

            if existing_user:
                print("Email already exists. Please double-check and try again.")
            else:
                # Use BigQuery's API to insert a new user into the 'users' table
                query = f'''
                        INSERT INTO `durable-height-412117.global_inflation.users` (FirstName, LastName, DateOfBirth, Location, Username, Password)
                        VALUES (@f_name, @l_name, @dob, @location, @username, @password)
                        '''

                query_job = client.query(query, job_config=job_config)
                query_job.result()  # This line waits for the job to complete.

                print("Registration successful!")
                break

        except Exception as e:
            print(f"Error during registration: {e}")

def logging():
    """Function for checking sign-in procedure"""
    while True:
        username = input("Enter your email: ").lower()
        password = getpass.getpass("Enter your password: ")

        if username == MANAGER_ACCOUNT["username"] and password == MANAGER_ACCOUNT["password"]:
            return MANAGER_ACCOUNT
        else:
            # Check if the user exists in BigQuery
            query = f'''
                    SELECT *
                    FROM `durable-height-412117.global_inflation.users`
                    WHERE username = '{username}' AND password = '{password}'
                    '''

            query_job = client.query(query)
            user = list(query_job)

            if user:
                user_dict = {
                    "FirstName": user[0]['FirstName'],
                    "LastName": user[0]['LastName'],
                    "DateOfBirth": user[0]['DateOfBirth'],
                    "Location": user[0]['Location'],
                    "Username": user[0]['Username'],
                    "Password": user[0]['Password']
                }
                print("Login successful!")
                return user_dict
            else:
                print("Invalid username or password. Please try again.")

        try_again = input("Do you want to try again? (y/n): ").lower()
        if try_again == 'n':
            break