import os
from metaflow import FlowSpec, step, retry
import pandas as pd
from sqlalchemy import create_engine, exc

class ETLFlow(FlowSpec):

    db_params = {
        'user': 'airbnb_user',
        'password': 'password',
        'host': 'localhost',
        'port': '5432',
        'database': 'airbnb'
    }

    @step
    def start(self):
        print("Starting ETL flow...")
        self.next(self.load_data)

    @step
    def load_data(self):
        # Define the file path
        file_path = os.path.join(os.path.dirname(__file__), '..', 'data', 'AB_NYC_2019.csv')
        
        # Check if the file exists
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File not found: {file_path}")
        
        # Load dataset
        self.df = pd.read_csv(file_path)
        print("Data loaded from CSV successfully.")
        self.next(self.extract_data)

    @retry
    @step
    def extract_data(self):
        try:
            # Connect to PostgreSQL
            engine = create_engine(f'postgresql://{self.db_params["user"]}:{self.db_params["password"]}@{self.db_params["host"]}:{self.db_params["port"]}/{self.db_params["database"]}')
            
            # Load data into PostgreSQL table
            self.df.to_sql('listings', engine, index=False, if_exists='replace')
            print("Data loaded into PostgreSQL successfully.")
        except exc.SQLAlchemyError as e:
            raise Exception(f"Failed to load data into PostgreSQL: {str(e)}")
        self.next(self.transform_data)

    @step
    def transform_data(self):
        try:
            # Data transformation
            self.df['date'] = pd.to_datetime(self.df['last_review']).dt.date
            self.df['time'] = pd.to_datetime(self.df['last_review']).dt.time
            self.avg_price_per_neighborhood = self.df.groupby('neighbourhood_group')['price'].mean().reset_index()
            self.df.fillna({'reviews_per_month': 0}, inplace=True)
            print("Data transformation completed successfully.")
        except Exception as e:
            raise Exception(f"Data transformation failed: {str(e)}")
        self.next(self.load_transformed_data)

    @retry
    @step
    def load_transformed_data(self):
        try:
            # Connect to PostgreSQL
            engine = create_engine(f'postgresql://{self.db_params["user"]}:{self.db_params["password"]}@{self.db_params["host"]}:{self.db_params["port"]}/{self.db_params["database"]}')
            
            # Load transformed data into new tables
            self.df.to_sql('transformed_listings', engine, index=False, if_exists='replace')
            self.avg_price_per_neighborhood.to_sql('avg_price_per_neighborhood', engine, index=False, if_exists='replace')
            print("Transformed data loaded into PostgreSQL successfully.")
        except exc.SQLAlchemyError as e:
            raise Exception(f"Failed to load transformed data into PostgreSQL: {str(e)}")
        self.next(self.end)

    @step
    def end(self):
        print("ETL process completed successfully.")

if __name__ == '__main__':
    ETLFlow()
