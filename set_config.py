import getpass

def mysql_config():
    # psw = getpass.getpass("Enter your mysql password: ")
    # db = input("Enter your database: ")
    return {
        'host': "localhost",
        'port': 3306,
        'user': 'root',
        'passwd': "mypassword",
        'db': "mydb",
        'auth_plugin': 'mysql_native_password' 
    }