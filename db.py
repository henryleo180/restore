import mysql.connector
import click
import pandas as pd
from sqlalchemy import create_engine
import os
from set_config import mysql_config

def mysql_engine():
    # Check if database exist otherwise create a new one
    db_config = mysql_config()
    conn = mysql.connector.connect(user=db_config['user'], password=db_config['passwd'],host=db_config['host'])
    cursor = conn.cursor()
    cursor.execute("create database if not exists "+db_config['db'])

    conn.close()

    return create_engine("mysql+pymysql://{user}:{passwd}@{host}:{port}/{db}".format(**db_config))

def import_all_file(path):
    engine = mysql_engine() # create_engine("mysql+pymysql://{user}:{passwd}@{host}:{port}/{db}".format(**mysql_config()))
    for file in os.listdir(path):
        if file.endswith('.csv'):
            data = pd.read_csv(path+'/'+file)
            # data.drop(index=0, inplace=True)
            table_name = str(file.split('.')[0])

            print("Load csv file "+file+". Now import it into database.")
            data.to_sql(table_name, engine, index=False, if_exists='replace')
            print("Importing csv file in table "+table_name+" success!")

def import_one_file(file):
    engine = mysql_engine() # create_engine("mysql+pymysql://{user}:{passwd}@{host}:{port}/{db}".format(**mysql_config()))

    data = pd.read_csv(file)
    print(data.head())
    # data.drop(index=0, inplace=True)
    table_name = str(file.split('.')[0].split('/')[-1])

    print("Load csv file "+file+". Now import it into database.")
    data.to_sql(table_name, engine, index=False, if_exists='replace')
    print("Importing csv file in table "+table_name+" success!")

@click.command()
@click.option('--all', is_flag=True, default=False,help='load all data file in your data directory')
@click.option('--path', default='data', help='Your path for data files.')
@click.argument('file', required=False)
def load_data_file(all, path, file):
    """Load csv data file into mysql database."""
    if all:
        import_all_file(path)
    elif file:
        import_one_file(file)
    else:
        click.echo("Please enter data file path or single data file.")

if __name__ == '__main__':

    load_data_file()