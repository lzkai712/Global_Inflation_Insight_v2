from user_management import logging, register, is_manager
from options import manager_interface, regular_user_interface

def main():
    print("Welcome to Global Inflation Insight. Login or register to start.")
    while True:
        choice = input("Choose an option (1: Login, 2: Register, q: Quit): ")

        if choice == '1':
            user = logging()
            if user:
                if is_manager(user.get("username") or user.get("Username")):
                    manager_interface()
                else:
                    regular_user_interface()
        elif choice == '2':
            register()
        elif choice.lower() == 'q':
            break
        else:
            print("Invalid choice. Please try again.")

if __name__ == "__main__":
    main()