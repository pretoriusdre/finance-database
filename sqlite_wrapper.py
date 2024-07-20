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
            uuid_col = [self._get_uuid7() for _ in range(len(df))]
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
            try:
                table_create_sql = self.get_table_ddl(table_name)
                table_drop_sql = f"DROP TABLE [{table_name}]"
                self._execute(table_drop_sql)
                self._execute(table_create_sql)
            except Exception:
                pass # Table does not exist and will be created.

            # Table was already deleted and recreated using correct DDL. Now just append.
            try:
                with sqlite3.connect(self.db_path) as conn:
                    df.to_sql(table_name, conn, if_exists='append', index=False)
            except Exception:
                # Incompatible schema. Fall back to replace
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


    def delete_table(self, table_name):
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
                self.delete_table(table_name)

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


    def _get_uuid7(self):
        """
        Generates a UUID7-compliant string.
        
        The leading 48 bits are the timestamp in milliseconds from Unix epoch, so the values will increase monotonically over time
        
        Returns:
            str: A UUID7-compliant string.
        """

        # This function written based on code by nickhobbs94 in an open pull request on the uuid7 repository by stevesimmons
        # https://github.com/stevesimmons/uuid7/pull/2
        # The uuid7 library was not used as it is seems to be unmaintained and not matching the current draft due to use of ns time resolution.


        ms = time.time_ns() // 1000000
        rand_a = int.from_bytes(os.urandom(2), byteorder='big')
        rand_b = int.from_bytes(os.urandom(8), byteorder='big')
        version = 0x07
        var = 2
        rand_a &= 0xfff
        rand_b &= 0x3fffffffffffffff
        uuid_bytes = ms.to_bytes(6, byteorder='big')
        uuid_bytes += ((version<<12)+rand_a).to_bytes(2, byteorder='big')
        uuid_bytes += ((var<<62)+rand_b).to_bytes(8, byteorder='big')
        return f"{uuid_bytes[:4].hex()}-{uuid_bytes[4:6].hex()}-{uuid_bytes[6:8].hex()}-{uuid_bytes[8:10].hex()}-{uuid_bytes[10:].hex()}"







if __name__ == "__main__":
    # Example usage: See example.ipynb
    pass