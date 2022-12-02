import pandas as pd
import sqlalchemy as sa


class Airlock():
    def __init__(self):
        self.AIRLOCK_HOST="smartmart-digital-consumer.c4jbufuformn.eu-central-1.redshift.amazonaws.com"
        self.AIRLOCK_PORT=5439
        self.AIRLOCK_USER="s1112230"
        self.AIRLOCK_PASSWD="s1112230PASS2567!"
        self.AIRLOCK_DRIVER="postgres+psycopg2"
        self.AIRLOCK_DATABASE="mio"

    def create_connection(self):
        connection_string = '{driver}://{user}:{passwd}@{host}:{port}/{database}'.format(
                    driver = self.AIRLOCK_DRIVER,
                    user = self.AIRLOCK_USER,
                    passwd = self.AIRLOCK_PASSWD,
                    host = self.AIRLOCK_HOST,
                    port = self.AIRLOCK_PORT,
                    database = self.AIRLOCK_DATABASE
                )
        self.connection = sa.create_engine(connection_string)
        self.connect = self.connection.connect()
    
    def close_connection(self):
        self.connect.close()
        
    def get_data(self, sql_string):
        df = pd.DataFrame()
        for chunk in  pd.read_sql_query(sql_string, self.connection, chunksize=100000):
            df = pd.concat([df, chunk], axis=0)
        return df 
    