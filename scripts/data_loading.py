import os
import pandas as pd
from sqlalchemy import create_engine

def load_data_to_postgres():
    # Load dataset
    df = pd.read_csv(os.path.join(os.path.dirname(__file__), '..', 'data', 'AB_NYC_2019.csv'))
    
    # PostgreSQL connection parameters
    db_params = {
        'user': 'airbnb_user',
        'password': 'password',
        'host': 'localhost',
        'port': '5432',
        'database': 'airbnb'
        }

    # Connect to PostgreSQL
    engine = create_engine(f'postgresql://{db_params["user"]}:{db_params["password"]}@{db_params["host"]}:{db_params["port"]}/{db_params["database"]}')

    
    # Load data into PostgreSQL table
    df.to_sql('listings', engine, index=False, if_exists='replace')
    print("Data loaded into PostgreSQL successfully.")

if __name__ == "__main__":
    load_data_to_postgres()
