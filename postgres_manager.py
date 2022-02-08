import psycopg2
from typing import List # needs to support different list's types
import pandas as pd
from config import POSTGRES_CREDS
from loguru import logger

#----------------------------------------------------------------------------------------
# Class to create connection, write to DB and execute queries
#----------------------------------------------------------------------------------------
class DbManager:

    def __init__(self):
        pass
    #------------------------------------------------------------------------------------
    @logger.catch
    def get_db_credentials(self):
        '''
        Method to get cursor and connection for given database
        '''
        for try_count in range(1, 11):
            try:
                connection = psycopg2.connect(POSTGRES_CREDS)
                if connection:
                    cursor = connection.cursor()
                    return connection, cursor
            except Exception as e:    
                if try_count in range(7, 10):
                    logger.debug(f'cant connect to DB || try {try_count}/10')
                    logger.error(e)
                if try_count == 11:
                    logger.error(f'cant connect to DB || last try')
                    raise
                continue
        return None, None
    #------------------------------------------------------------------------------------
    def connect_to_db(func):
        """
        Decorator for getting connection and cursor for class methods
        Connection closes automatically
        :return:
        """

        def wrapper(self, *args,**kwargs):
            try:
                conn, cur = self.get_db_credentials()
                return func(self, cursor=cur, connection=conn, *args, **kwargs)
            except Exception as e:
                logger.debug(f"Error during working in func - {func.__name__}")
                logger.error(e)
                conn.rollback()
            finally:
                cur.close()
                conn.close()

        return wrapper
    #------------------------------------------------------------------------------------
    @connect_to_db
    def write_df_to_db(self,
                    df: pd.DataFrame(),
                    table_name: str,
                    create_table:bool = False,
                    primary_keys: List = [],
                    on_conflict: bool = False,
                    connection=None,
                    cursor=None
                    ) -> bool:
        """
        Method for writing to DB
        :param to_db_list: list_of_lists 
        :param table_name: str
        :param primary_keys: --- LIST of table's primary_key.
        :param on_conflict: --- optional
        :param connection: --- given from decorator
        :param cursor: --- given from decorator
        :return:
        """
        if create_table:
            DbTablesCreator().create_table_from_dataframe(df,table_name)
        column_names = df.columns.tolist()
        column_names = [column_name.replace(' ','_') for column_name in column_names]
        if on_conflict:
            update_string = ','.join([f"{column_name} = excluded.{column_name}" for column_name in column_names if column_name not in primary_keys])
        try:
            query = f"""INSERT INTO {table_name} ({','.join(column_names)}) VALUES """
            conflict_statement = """ ON CONFLICT DO NOTHING"""
            if on_conflict:
                conflict_statement = """ ON CONFLICT ({0}) DO UPDATE SET {1};""".format(','.join(primary_keys), update_string)
            values = ''
            for val in df.values.tolist():
                val = str(val)
                val = val.replace("'", '').replace("`", '').replace("’", '').replace("\\", '').replace("'",'').replace('\u200b','')
                values += f"'{val}',"
            query += f""" ({values[:-1]}),"""      
            cursor.execute(query[:-1] + conflict_statement)
            connection.commit()
        except Exception as e:
            logger.error(e)
            return False
        return True
    #------------------------------------------------------------------------------------
    @connect_to_db
    def write_list_of_dicts_to_db(self,
                    list_of_dicts: List[dict],
                    table_name: str,
                    primary_keys: List = [],
                    create_table: bool = False,
                    on_conflict: bool = False,
                    connection=None,
                    cursor=None
                    ) -> bool:
        """
        Method for writing to DB
        :param to_db_list: list_of_lists 
        :param table_name: str
        :param primary_keys: --- LIST of table's primary_key.
        :param on_conflict: --- optional
        :param connection: --- given from decorator
        :param cursor: --- given from decorator
        :return:
        """
        if create_table:
            df = pd.DataFrame.from_records(list_of_dicts)
            DbTablesCreator().create_table_from_dataframe(df,table_name)
        column_names = list_of_dicts[0].keys()
        column_names = [column_name.replace(' ','_') for column_name in column_names]
        if on_conflict:
            update_string = ','.join([f"{column_name} = excluded.{column_name}" for column_name in column_names if column_name not in primary_keys])
        try:
            query = f"""INSERT INTO {table_name} ({','.join(column_names)}) VALUES """
            conflict_statement = """ ON CONFLICT DO NOTHING"""
            if on_conflict:
                conflict_statement = """ ON CONFLICT ({0}) DO UPDATE SET {1};""".format(','.join(primary_keys), update_string)
            for dicto in list_of_dicts:
                values = ''
                for val in dicto.values():
                    val = str(val)
                    val = val.replace("'", '').replace("`", '').replace("’", '').replace("\\", '').replace("'",'').replace('\u200b','')
                    values += f"'{val}',"
                query += f""" ({values[:-1]}),"""      
            cursor.execute(query[:-1] + conflict_statement)
            connection.commit()
        except Exception as e:
            logger.error(e)
            return False
        logger.success('Data successfully added to DB')
        return True
    #------------------------------------------------------------------------------------
    @connect_to_db
    def execute_query(self,
                      query: str,
                      connection=None, 
                      cursor=None
                      ) -> bool:
        '''
        Don't forget to specify param "query" while calling
        ;param query: str
        :param connection: --- given from decorator
        :param cursor: --- given from decorator
        '''
        try:
            cursor.execute(query)
            connection.commit()
            logger.debug('Query successfully executed')
            return True
        except Exception as e:
            logger.debug('Cannot not execute query due to following error : ')
            logger.error(e)
            return False
    #------------------------------------------------------------------------------------
    @connect_to_db
    def select_query(self,
                     query:str,
                     as_type:str = '',
                     connection=None, 
                     cursor=None 
                     ):
        '''
        Don't forget to specify param "query" while calling
        :param query: str
        :param connection: --- given from decorator
        :param cursor: --- given from decorator
        :param as_type: --- str, default '' . Type to be returned, ‘dict’ or ‘dataframe’
        '''
        try:
            cursor.execute(query)
            if not as_type:
                text_lists = cursor.fetchall()
                logger.success('Successfully fetched info from DB')
                return text_lists
            elif as_type:
                columns = cursor.description 
                resulted_dict = [{columns[index][0]:column_value for index, column_value in enumerate(value)} for value in cursor.fetchall()]
                if as_type == 'dict':
                    logger.success('Successfully fetched info from DB')
                    return resulted_dict
                elif as_type == 'dataframe':
                    logger.success('Successfully fetched info from DB')
                    resulted_df = pd.DataFrame.from_records(resulted_dict)
                    return resulted_df
                else:
                    return 'Wrong type'
        except Exception as e:
            logger.debug('Cannot not select query due to following error : ')
            logger.error(e)
            return None
    #------------------------------------------------------------------------------------

#----------------------------------------------------------------------------------------
# Class to either create table with hardrcoded fields or create table depended on given dataframe
#----------------------------------------------------------------------------------------
class DbTablesCreator:

    def __init__(self):
        pass

    def create_table(self,  
                     table_name:str,
                     drop_existing:bool = False,
                     ) -> bool:
        '''
        :param table name: str type table name
        :param drop_existing: --- optional. False by default
        '''
        #--------------------------------------------------------------------------------
        # Generating query for creating table
        query = """
                CREATE TABLE IF NOT EXISTS %s
                (
                    item_id bigint,
                    item_count bigint,
                    date_added timestamp,
                    last_updated timestamp,
                    PRIMARY KEY (item_id)
                )
                """ % table_name
        #--------------------------------------------------------------------------------
        # Dropping existing table if needed
        if drop_existing:
            DbManager().execute_query('''DROP TABLE IF EXISTS %s''' % table_name) 
        #--------------------------------------------------------------------------------
        # Executing statement with DbManager() method
        if DbManager().execute_query(query):
            print(f'Table {table_name} was successfully created')
            return True
        print(f'Failed to create {table_name} table')
        return False
    #------------------------------------------------------------------------------------
    def create_table_from_dataframe(self, 
                                    df:pd.DataFrame(),
                                    table_name:str,
                                    drop_existing:bool = False,
                                    ) -> bool:
        '''
        :param df: given DataFrame()
        :param table name: str type table name
        :param drop_existing: --- optional. False by default
        '''
        #--------------------------------------------------------------------------------
        # Creating dict with names and types
        columns_names,columns_dtypes = df.columns.values,df.dtypes
        columns_info_dict = dict(zip(columns_names,columns_dtypes))
        #--------------------------------------------------------------------------------
        # Updating dict with postrgres-compatible types
        for column_name,column_dtype in columns_info_dict.items():
            if columns_info_dict[column_name] == 'int64':
                columns_info_dict[column_name] = 'int'
            elif columns_info_dict[column_name] == 'float64':
                columns_info_dict[column_name] = 'float'
            elif columns_info_dict[column_name] == 'bool':
                columns_info_dict[column_name] = 'boolean'
            else:
                columns_info_dict[column_name] = 'varchar'
        #--------------------------------------------------------------------------------
        # Generating statement for creating table
        createTableStatement = 'CREATE TABLE IF NOT EXISTS %s (' % table_name
        for column_name,column_dtype in columns_info_dict.items():
            createTableStatement = createTableStatement + '\n' + column_name + ' ' + column_dtype + ','
        createTableStatement = createTableStatement[:-1] + ' );'
        #--------------------------------------------------------------------------------
        # Dropping existing table if needed
        if drop_existing:
            DbManager().execute_query('''DROP TABLE IF EXISTS %s''' % table_name) 
        #--------------------------------------------------------------------------------
        # Executing statement with DbManager() method
        if DbManager().execute_query(createTableStatement):
            print(f'Columns for {table_name} table were successfully created')
            return True
        print(f'Failed to create columns for {table_name} table')
        return False