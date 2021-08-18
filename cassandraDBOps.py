from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import pandas as pd
import json
from logger_class import Logger
class DBOps:

    def __init__(self, config_path, auth_id, auth_secret):
        """
        This function reads the cloud cluster config, auth id and token from configuration folder
        """
        try:
            self.cloud_config = {'secure_connect_bundle': config_path}
            self.auth_id = auth_id
            self.auth_secret = auth_secret
        except Exception as e:
            Logger('test.log').logger('ERROR', f'unable to read config file\n {str(e)}')

    def getCluster(self):
        """
        This function creates the client object for connection purpose
        """
        try:
            cluster = Cluster(cloud=self.cloud_config, auth_provider=PlainTextAuthProvider(self.auth_id, self.auth_secret))
            return cluster
        except Exception as e:
            Logger('test.log').logger('ERROR', f'unable to connect to cloud cluster \n {str(e)}')

    def closeSession(self, session):
        """
        This function closes the cluster connection
        """
        try:
            session.shutdown()
        except Exception as e:
            Logger('test.log').logger('ERROR', f'error when shutting down the session \n {str(e)}')

    def createSession(self):
        """
        Create a cluster session
        """
        try:
            session = self.getCluster().connect()
            return session
        except Exception as e:
            Logger('test.log').logger('ERROR', f'error establishing a session \n {str(e)}')

    def isKeyspacePresent(self, key_name):
        """
        This function is to check whether the keyspace is available in the db
        """
        try:
            session = self.createSession()
            if key_name in [i[0] for i in session.execute('SELECT * FROM system_schema.keyspaces;')]:
                self.closeSession(session)
                return True
            else:
                self.closeSession(session)
                return False
        except Exception as e:
            Logger('test.log').logger('ERROR', f'unable to check whether the keyspace is available or not \n {str(e)}')

    def createKeyspace(self, key_name):
        """
        This function creates a new keyspace if not available in the cluster
        """
        try:
            key_status = self.isKeyspacePresent(key_name)
            if not key_status:
                session = self.createSession()
                key_space = session.execute(f'CREATE KEYSPACE {key_name};')
                self.closeSession(session)
                return key_space
            else:
                Logger('test.log').logger('INFO', f'keyspace {key_name} is already available')
        except Exception as e:
            print(e)
            Logger('test.log').logger('ERROR', f'Failed creating keyspace\n + {str(e)}')

    def dropKeyspace(self, key_name):
        """
        This function drops the keyspace but creates a snapshot for recovery
        """
        try:
            session = self.createSession()
            if self.isKeyspacePresent(key_name):
                session.execute(f'DROP KEYSPACE IF EXISTS {key_name};')
                self.closeSession(session)
            else:
                self.closeSession(session)
                Logger('test.log').logger('INFO', f'keyspace {key_name} is not available')
        except Exception as e:
            Logger('test.log').logger('ERROR', f'Failed deleting keyspace \n + {str(e)}')

    def isTablePresent(self, key_name, table_name):
        """
        this function checks whether the table is available inside the keyspace
        """
        try:
            session = self.createSession()
            query = f"""
                SELECT * FROM system_schema.tables WHERE keyspace_name = '{key_name}';
            """
            if table_name in [i[1] for i in session.execute(query)]:
                self.closeSession(session)
                return True
            else:
                self.closeSession(session)
                return False
        except Exception as e:
            Logger('test.log').logger('ERROR', f'Failed checking table is available \n + {str(e)}')

    def createTable(self, key_name, table_name):
        """
        Function creates a table using keyspace and table name
        """
        try:
            session = self.createSession()
            status = self.isTablePresent(key_name, table_name)
            if not status:
                query = f"""
                    CREATE TABLE IF NOT EXISTS {key_name}.{table_name} (id uuid, product_name text, product_searched text, price text, offer_details text, discount_percent text, EMI text, rating text, comment text, customer_name text, review_age text, PRIMARY KEY(id));
                """
                table = session.execute(query)
                self.closeSession(session)
                return table
            else:
                Logger('test.log').logger('INFO', f'Table already exists')
        except Exception as e:
            Logger('test.log').logger('ERROR', f'Unable to create a table \n + {str(e)}')

    def dropCollection(self, key_name, table_name):
        """
        Function deletes the table from keyspace
        """
        try:
            session = self.createSession()
            status = self.isTablePresent(key_name, table_name)
            if status:
                session.execute(f"DROP TABLE IF EXISTS {key_name}.{table_name};")
                Logger('test.log').logger('INFO', f'Table {table_name} in keyspace {key_name} has been deleted')
                self.closeSession(session)
            else:
                Logger('test.log').logger('INFO', f'Table {table_name} is not available in keyspace {key_name}')
                self.closeSession(session)
        except Exception as e:
            Logger('test.log').logger('ERROR', f'Unable to delete table \n {str(e)}')

    def insertOneRecord(self, key_name, table_name, record):
        """
        Function inserts one record into the table in keyspace
        """
        try:
            session = self.createSession()
            status = self.isTablePresent(key_name, table_name)
            table_key = "id, "+', '.join([str(i) for i in record.keys()])
            values = "uuid(), "+", ".join([f"'{str(i)}'" for i in record.values()])
            if status:
                query = f"""
                    INSERT INTO {key_name}.{table_name}({table_key}) values ({values});     
                """
                print(query)
                session.execute(query)
                self.closeSession(session)
                Logger('test.log').logger('INFO', f'record inserted into table {table_name} in keyspace {key_name}')
            else:
                table = self.createTable(key_name, table_name)
                query = f"""INSERT INTO {key_name}.{table_name}({table_key}) values ({values});"""
                session.execute(query)
                self.closeSession(session)
                Logger('test.log').logger('INFO', f'record inserted into table {table_name} in keyspace {key_name}')
        except Exception as e:
            Logger('test.log').logger('ERROR', f'record insertion failed \n {str(e)}')

    def insertMultiRecords(self, key_name, table_name, records):
        """
        Function inserts multiple records into a table in the key space
        records need to be a list of dictionaries for the records that need to be added
        """
        try:
            for record in records:
                self.insertOneRecord(key_name, table_name, record)
        except Exception as e:
            Logger('test.log').logger('ERROR', f'record insertion failed \n {str(e)}')

    def findFirstRecord(self, key_name, table_name, query=None):
        """
        Function returns the first record in the table
        :param key_name:
        :param table_name:
        :param query:
        :return:
        """
        try:
            session = self.createSession()
            status = self.isTablePresent(key_name, table_name)
            if status:
                query = f"SELECT * from {key_name}.{table_name} WHERE {list(query.keys())[0]}='{list(query.values())[0]}' LIMIT 1 ALLOW FILTERING"
                print(query)
                rows = session.execute(query)
                self.closeSession(session)
                Logger('test.log').logger('INFO', f'query was executed in {table_name} in keyspace {key_name}')
                return [row for row in rows]
            else:
                table = self.createTable(key_name, table_name)
                query = f"SELECT * from {key_name}.{table_name} WHERE {list(query.keys())[0]}='{list(query.values())[0]}' LIMIT 1 ALLOW FILTERING"
                rows = session.execute(query)
                self.closeSession(session)
                Logger('test.log').logger('INFO', f'query was executed in {table_name} in keyspace {key_name}')
                return [row for row in rows]
        except Exception as e:
            Logger('test.log').logger('ERROR', f'error with the query \n {str(e)}')

    def findAllRecords(self, key_name, table_name):
        """
        Function returns all the records in the table in the keyspace
        :param key_name:
        :param table_name:
        :return:
        """
        try:
            session = self.createSession()
            status = self.isTablePresent(key_name, table_name)
            if status:
                query = f"SELECT * from {key_name}.{table_name}"
                print(query)
                rows = session.execute(query)
                self.closeSession(session)
                Logger('test.log').logger('INFO', f'query was executed in {table_name} in keyspace {key_name}')
                return [row for row in rows]
            else:
                table = self.createTable(key_name, table_name)
                query = f"SELECT * from {key_name}.{table_name}"
                rows = session.execute(query)
                self.closeSession(session)
                Logger('test.log').logger('INFO', f'query was executed in {table_name} in keyspace {key_name}')
                return [row for row in rows]
        except Exception as e:
            Logger('test.log').logger('ERROR', f'error with the query \n {str(e)}')

    def getDFfromDB(self, key_name, table_name):
        """
        Function creates and returns a pandas dataframe from the cassandra db
        :param key_name:
        :param table_name:
        :return:
        """
        try:
            status = self.isTablePresent(key_name, table_name)
            if status:
                records = self.findAllRecords(key_name, table_name)
                df = pd.DataFrame(records)
                return df[[col for col in df.columns if col != 'id']]
            else:
                Logger('test.log').logger('INFO', f'{table_name} is not available in the {key_space} db')
        except Exception as e:
            Logger('test.log').logger('ERROR', f'creating a df from cassandra \n {str(e)}')

    def saveDFIntoDB(self, key_name, table_name, dataframe):
        """
        Function inserts all the records from the dataframe into the cassandra db
        :param key_name:
        :param table_name:
        :param dataframe:
        :return:
        """
        try:
            df_dict = json.loads(dataframe.T.to_json())
            for index in df_dict:
                self.insertOneRecord(key_name, table_name, df_dict[index])
            Logger('test.log').logger('INFO', f'all records have been inserted')
        except Exception as e:
            Logger('test.log').logger('ERROR', f'error saving data from df into db \n {str(e)}')

    def getResultToDisplayOnBrowser(self, key_name, table_name):
        """
        Function returns final result to display on the browser
        :param key_name:
        :param table_name:
        :return:
        """
        try:
            return self.findAllRecords(key_name, table_name)
        except Exception as e:
            Logger('test.log').logger('ERROR', f'Something went wrong on getting result from db \n {str(e)}')