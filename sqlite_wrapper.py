"""A wrapper class to interact with an SQLite database, providing methods for common database operations."""

import sqlite3
import os
import time
import string

import numpy as np
import pandas as pd

class SQLiteWrapper:
    """
    A wrapper class to interact with an SQLite database, providing methods
    for common database operations.

    Attributes:
        db_path (str): Path to the SQLite database file.
    """
    def __init__(self, db_path, create=False):
        """
        Initializes an instance of SQLiteWrapper.

        Args:
            db_path (str): Path to the SQLite database file.
            create (bool): Whether to create the database if it doesn't exist. Defaults to False.
        """
        self.db_path = db_path
        if not create:
            assert os.path.exists(self.db_path)


    def execute(self, statement, parameters=None, fetch=False):
        """
        Executes a provided SQL statement with optional parameters.

        Args:
            statement (str): The SQL statement to execute.
            parameters (tuple, optional): Parameters to use with the statement. Defaults to None.
            fetch (bool, optional): Whether to fetch and return results. Defaults to False.

        Returns:
            list: Results of the query if fetch is True, otherwise None.

        Raises:
            sqlite3.Error: If an error occurs while executing the statement.
        """
        try:
            return_val = self._execute(statement, parameters, fetch)
            print("Statement executed successfully.")
            return return_val
        except sqlite3.Error as e:
            print(f"Error while executing statement: {e}")
            raise e

    def _execute(self, statement, parameters=None, fetch=False):
        """Internal function for executing statements"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            if parameters:
                cursor.execute(statement, parameters)
            else:
                cursor.execute(statement)

            if fetch:
                return cursor.fetchall()
            else:
                conn.commit()  # Not required because of the context manager, including for clarity


    def _executemany(self, statement, parameters=None):
        """Internal function for executing statements in batch"""
        with sqlite3.connect(self.db_path) as conn:
            if parameters:
                conn.executemany(statement, parameters)
            else:
                conn.executemany(statement)
            conn.commit()  # Not required because using context manager, including for clarity.


    def table_exists(self, table_name):
        """
        Checks if a given table exists in the database.

        Args:
            table_name (str): Name of the table to check.

        Returns:
            bool: True if the table exists, False otherwise.
        """
        table_name = self._sanitise_input(table_name)
        try:
            query = f"SELECT 1 FROM [{table_name}] LIMIT 1"
            self._execute(query)
            return True
        except sqlite3.Error:
            return False


    def save_data(
        self,
        df,
        table_name,
        if_exists='replace',
        auto_create=False, # Generally not used as there would be no constraints
        unique_key=None, # Used for upsert operation. Provide a list of column names used as keys.
        auto_add_id=False
        ):
        """
        Saves data from a DataFrame to a specified table in the database.

        Args:
            df (DataFrame): The data to save.
            table_name (str): The name of the table to save data to.
            if_exists (str, optional): Action if table exists ('fail', 'replace', 'append', 'upsert').
                                       Defaults to 'replace'.
            auto_create (bool, optional): Whether to create the table if it doesn't exist. Defaults to False.
            unique_key (str or list, optional): Column names used as keys for upsert operations. Defaults to None.
            auto_add_id (bool, optional): Whether to automatically add a UUID7 ID column. Defaults to False.

        Raises:
            ValueError: If the specified table does not exist and auto_create is set to False.
            ValueError: If unique_key is not provided for upsert operations.
        """

        table_name = self._sanitise_input(table_name)

        if_exists_allowable = ('fail', 'replace', 'append', 'upsert')
        if if_exists not in if_exists_allowable:
            raise ValueError(f'if_exists parameter not in {if_exists_allowable}')

        if not self.table_exists(table_name) and not auto_create:
            raise ValueError(f'Table {table_name} does not exist and auto_create is set to False')

        if auto_add_id:
            df = df.copy()
            uuid_col = [self.__class__._get_uuid7() for _ in range(len(df))]
            df.insert(0, 'id',  uuid_col)

        if if_exists == 'upsert':
            if not unique_key:
                raise ValueError("unique_key must be provided for upsert operations")

            if isinstance(unique_key, str):
                unique_key = [unique_key] # Turn the string into a single element list

            column_list = self._sanitise_input_list(df.columns)
            key_list = self._sanitise_input_list(unique_key)

            columns_str = ', '.join([f'[{col_name}]' for col_name in column_list])
            placeholders_str = ', '.join(['?' for col_name in column_list])
            keys_str = ', '.join([ f'[{key_name}]' for key_name in key_list])
            set_clause_str = ', '.join([f'[{col}] = excluded.[{col}]' for col in column_list if col not in key_list])
            
            upsert_sql = f"""
                INSERT INTO [{table_name}] ({columns_str})
                VALUES ({placeholders_str})
                ON CONFLICT ({keys_str})
                DO UPDATE SET {set_clause_str}
                """

            def convert_types(t):
                converted_tuple = tuple(
                    int(x) if isinstance(x, np.integer) else
                    float(x) if isinstance(x, np.floating) else
                    x for x in t
                    )
                return converted_tuple

            records = [convert_types(record) for record in df.to_records(index=False)]

            try:
                self._executemany(upsert_sql, records)
            except Exception as e:
                self._handle_exception(e)

        elif if_exists == 'replace':
            # Pandas replace method will remove any DDL, so this is used to retain it.
            if self.table_exists(table_name):
                try:
                    table_delete_sql = f"DELETE FROM [{table_name}]"
                    self._execute(table_delete_sql)
                except Exception as e:
                    print(f"Error deleting the data already saved to table '{table_name}', likely due to a foreign key constraint.")
                    self._handle_exception(e)
        
            # Table was cleared, retaining its original DDL and contraints. Now use append to add the new data.
            try:
                with sqlite3.connect(self.db_path) as conn:
                    df.to_sql(table_name, conn, if_exists='append', index=False)
            except Exception:
                # Incompatible schema. Fall back to replace. This will replace the table.
                with sqlite3.connect(self.db_path) as conn:
                    df.to_sql(table_name, conn, if_exists='replace', index=False)
                

        elif if_exists in ('append', 'fail'):
            with sqlite3.connect(self.db_path) as conn:
                df.to_sql(table_name, conn, if_exists=if_exists, index=False)


        print(f"Data saved to table '{table_name}' successfully.")


    def get_table(self, table_name):
        """
        Retrieves all data from a specified table.

        Args:
            table_name (str): The name of the table to retrieve data from.

        Returns:
            DataFrame: The retrieved data as a DataFrame.
        """
        table_name = self._sanitise_input(table_name)
        
        return self.get_query(f"SELECT * FROM [{table_name}]")


    def get_query(self, query, parameters=None):
        """
        Executes a SQL query and returns the result as a DataFrame.

        Args:
            query (str): The SQL query to execute.
            parameters (tuple, optional): Parameters to use with the query. Defaults to None.

        Returns:
            DataFrame: The result of the query as a DataFrame.

        Raises:
            Exception: If an error occurs while executing the query.
        """
        try:
            with sqlite3.connect(self.db_path) as conn:
                df = pd.read_sql_query(query, conn, params=parameters)
            return df
        except Exception as e:
            self._handle_exception(e)

    def get_all_table_names(self):
        """
        Retrieves the names of all tables in the database.

        Returns:
            list: A list of table names.
        """
        query = """
            SELECT name
            FROM sqlite_master
            WHERE type = 'table';
            """
        try:
            records = self._execute(query, fetch=True)
            return [record[0] for record in records]
        except Exception as e:
            self._handle_exception(e)


    def get_table_ddl(self, table_name):
        """
        Retrieves the DDL (Data Definition Language) statement for a specified table.

        Args:
            table_name (str): The name of the table to retrieve the DDL for.

        Returns:
            str: The DDL statement for the specified table.

        Raises:
            ValueError: If the specified table does not exist.
        """
        table_name = self._sanitise_input(table_name)
        query = f"""
            SELECT sql
            FROM sqlite_master
            WHERE type = 'table' AND name = '{table_name}';
            """
        try:
            records = self._execute(query, fetch=True)
        except Exception as e:
            self._handle_exception(e)

        if len(records) > 0:
            return records[0][0] # First record then first column
        else:
            raise ValueError(f"Table '{table_name}' does not exist.")


    def drop_table(self, table_name):
        """
        Deletes a specified table from the database.

        Args:
            table_name (str): The name of the table to delete.

        Raises:
            ValueError: If the specified table does not exist.
        """
        table_name = self._sanitise_input(table_name)

        if self.table_exists(table_name):
            drop_query = f"DROP TABLE [{table_name}]"
            self._execute(drop_query)
        else:
            raise ValueError(f"Table '{table_name}' does not exist.") 


    def flush(self):
        """
        Deletes all tables in the database after a confirmation prompt.
        """
        confirmation = input('This function will completely wipe the database and cannot be undone. Type "Delete" to confirm:\n')
        confirmation = confirmation.strip('"').upper()
        if confirmation == 'DELETE':
            tables = self.get_all_table_names()
            for table_name in tables:
                self.drop_table(table_name)

            print("Database wiped successfully.")
        else:
            print("Operation cancelled.")



    def _sanitise_input(self, text):
        """
        Sanitizes input text to prevent SQL injection in dynamic DDL queries.

        Args:
            text (str): The text to sanitize.

        Returns:
            str: The sanitized text. Only alphanumeric and '.@-_' characters are allowed.
        """
        allowable_chars = string.ascii_letters + string.digits + '.@-_'
        sanitised_text = ''.join([char if char in allowable_chars else '_' for char in text])
        return sanitised_text


    def _sanitise_input_list(self, array):
        """
        Sanitizes a list of input texts.

        Args:
            array (list): List of text items to sanitize.

        Returns:
            list: List of sanitized text items.
        """
        return [self._sanitise_input(item) for item in array]


    def _handle_exception(self, e, raise_it=True):
        print(f'Sql error: {" ".join(e.args)}')
        print(f'Exception class is: {e.__class__}')
        if raise_it:
            raise e

    @classmethod
    def _get_uuid7(cls, unix_ts_ms=None, rand_a=None, rand_b=None):

        """A lightweight UUID7 implementation based on the draft UUIDv7 standard, retrieved 20 July 2024
        
        Refer to RFC 9562 at https://www.rfc-editor.org/rfc/rfc9562

        Bit allocation:
        0               1  <- Octets -> 2               3               4
        0 1 2 3 4 5 6 7 0 1 2 3 4 5 6 7 0 1 2 3 4 5 6 7 0 1 2 3 4 5 6 7 0
        +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
        |                           unix_ts_ms                          |  <- Octets 0,1,2,3
        +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
        |          unix_ts_ms           |  ver  |       rand_a          |  <- Octets 4,5,6,7
        +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
        |var|                        rand_b                             |  <- Octets 8,9,10,11
        +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
        |                            rand_b                             |  <- Octets 12,13,14,15
        +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+

                                            Grand total = 16 octets x 8 bits/octet = 128 bits
        
        Note: The UUID7 implementation currently available on PyPi is not correct as it is using nanosecond time resolution.
        https://pypi.org/project/uuid7/
        https://github.com/stevesimmons/uuid7/issues/1
        """
        
        # The current Unix timestamp in milliseconds
        if not unix_ts_ms:
            unix_ts_ms = time.time_ns() // 1000000
        
        # Random data
        if not rand_a:
            rand_a = int.from_bytes(os.urandom(2), byteorder='big') # 2 bytes = 16 bits of random data
        if not rand_b:
            rand_b = int.from_bytes(os.urandom(8), byteorder='big') # 8 bytes = 64 bits of random data
        
        # Fixed parameters for UUIDv7
        ver = 7 # Binary value: 0b0111
        var = 2 # Binary value: 0b10

        # Mask all inputs with zeroes to ensure they are sufficiently long.
        unix_ts_ms &= 0xFFFFFFFFFFFF  # 48 bits
        ver &= 0xF  # 4 bits
        rand_a &= 0xFFF  # 12 bits
        var &= 0x3  # 2 bits
        rand_b &= 0x3FFFFFFFFFFFFFFF # 62 bits

        uuid_bytes = unix_ts_ms.to_bytes(6, byteorder='big') # 6 bytes = 48 bits
        uuid_bytes += ((ver << 12) + rand_a).to_bytes(2, byteorder='big') # Total 2 bytes (16 bits). The 4 bit of ver is shifted by 12 bits to the start.
        uuid_bytes += ((var << 62) + rand_b).to_bytes(8, byteorder='big') # Total 8 bytes (64 bits). The 2 bits of var is shifted by 62 bits to the start.

        return f"{uuid_bytes[:4].hex()}-{uuid_bytes[4:6].hex()}-{uuid_bytes[6:8].hex()}-{uuid_bytes[8:10].hex()}-{uuid_bytes[10:].hex()}"
    
    @classmethod
    def _test_uuid(cls): 
        """This function is used to validate the test vector provided in the RFC documentation
        https://www.ietf.org/rfc/rfc9562.html#name-example-of-a-uuidv7-value
        """
        test_vector = {
            'unix_ts_ms':0x017F22E279B0,
            'rand_a':0xCC3,
            'rand_b': 0x18C4DC0C0C07398F
        }
        expected_output = '017F22E2-79B0-7CC3-98C4-DC0C0C07398F'.lower()
        is_compliant = cls._get_uuid7(**test_vector) == expected_output
        return is_compliant


assert SQLiteWrapper._test_uuid()

