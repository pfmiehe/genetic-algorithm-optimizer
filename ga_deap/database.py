import sys 
import pyodbc
import mysql.connector
from mysql.connector.pooling import MySQLConnectionPool
import os
import utils
import numpy as np
import copy
import logging

logger = logging.getLogger(__name__)

pyodbc.pooling = None


class Database:    

    def __init__(self, connection_string, reset_indexes=True):
        
        # Columns with PK and FK
        self.keys = {
            'customer': ['c_custkey', 'c_nationkey'],
            'lineitem': ['l_orderkey', 'l_linenumber', 'l_partkey', 'l_suppkey'],
            'nation': ['n_nationkey', 'n_regionkey'],
            'orders': ['o_orderkey', 'o_custkey'],
            'part': ['p_partkey'],
            'partsupp': ['ps_partkey', 'ps_suppkey'],
            'region': ['r_regionkey'],
            'supplier': ['s_suppkey', 's_nationkey']
        }
        logger.debug(self.keys)

        # Only columns used in queries
        self.tables = {
            'customer': ['c_name', 'c_address', 'c_comment'],
            'lineitem': ['l_extendedprice', 'l_linestatus', 'l_tax',
                        'l_linenumber', 'l_comment'],
            'nation': ['n_comment'],
            'orders': ['o_orderpriority', 'o_shippriority', 'o_clerk', 'o_totalprice'],
            'part': ['p_mfgr', 'p_retailprice', 'p_comment'],
            'partsupp': ['ps_comment'],
            'region': ['r_comment'],
            'supplier': ['s_name', 's_address', 's_phone', 's_acctbal']
        }                    

        self.connection_string = connection_string
        self.flat_state = self.get_column_list()        
        self.state_size = len(self.flat_state)
        self.initial_state = self.get_current_state()
        logger.debug(f'Initial State: {self.initial_state}')
        logger.debug(f'Current state size: {self.state_size}')

        if reset_indexes:
            self.reset_indexes()            
    
    def get_column_list(self):
        flat_columns = []
        for table, columns in sorted(self.tables.items()):
            for j, column in enumerate(columns):
                flat_columns.append(f'{table}.{column}')
        return flat_columns
                
    def state_to_vector(self, state):
        vector = np.zeros((self.state_size))
        k = 0
        for table, columns in sorted(self.tables.items()):
            for j, column in enumerate(columns):
                vector[k] = state[table][column]
                k += 1

        return vector

    def vector_to_state(self, vector):
        k = 0
        state = copy.deepcopy(self.initial_state)
        for table, columns in sorted(self.tables.items()):
            for j, column in enumerate(columns):
                state[table][column] = vector[k]
                k += 1

        return state

    def get_table_indexed_columns(self, table):
        self.conn = pyodbc.connect(self.connection_string)
        self.cur = self.conn.cursor()
        self.cur.execute('SHOW INDEXES FROM %s;' % table)
        table_indexes = list()
        for index in self.cur.fetchall():
            table_indexes.append(index[4])
        self.conn.commit()
        self.cur.close()
        self.conn.close()
        return sorted(table_indexes)

    def get_table_columns(self, table):
        self.conn = pyodbc.connect(self.connection_string)
        self.cur = self.conn.cursor()
        self.cur.execute('SHOW COLUMNS FROM %s;' % table)
        table_columns = list()
        for row in self.cur.fetchall():
            if row[0] not in self.tables:
                table_columns.append(row[0])
        self.conn.commit()
        self.cur.close()
        self.conn.close()
        return sorted(table_columns)

    # USE THIS FUNCTION FOR ALL TABLES OF TPC-H
    def get_current_state(self):
        indexes_map = dict()
        for table in self.tables.keys():
            indexes_map[table] = dict()
            indexed_columns = self.get_table_indexed_columns(table)
            table_columns = self.get_table_columns(table)
            for column in table_columns:
                indexes_map[table][column] = 0
                for index in indexed_columns:
                    if column == index:
                        indexes_map[table][column] = 1

        return indexes_map
    
    def get_current_state_vector(self,):
        state = self.get_current_state()
        return self.state_to_vector(state)

    # RETURN COLUMNS OF SOME TABLE
    def get_columns(self, table):
        columns = list()
        self.conn = pyodbc.connect(self.connection_string)
        self.cur = self.conn.cursor()
        self.cur.execute('SHOW COLUMNS FROM %s;' % table)
        for row in self.cur.fetchall():
            # if row[0] not in self.lineitem:
            columns.append(row[0])
        self.conn.commit()
        self.cur.close()
        self.conn.close()
        return columns

    # RETURN LIST OF THE COLUMNS OF ALL TABLES
    def get_list_columns(self):
        columns = []
        table_columns = []
        for table in self.get_current_state():
            table_columns.append(self.get_columns(table))
        for i in range(0, len(table_columns)):
            for column in table_columns[i]:
                columns.append(column)
        return columns    

    def drop_index(self, column, table):
        command = ("DROP INDEX idx_%s ON %s;" % (column, table))
        logger.debug(command)

        try:
            self.conn = pyodbc.connect(self.connection_string)
            self.cur = self.conn.cursor()
            self.cur.execute(command)
            self.conn.commit()
            self.cur.close()
            self.conn.close()

        except pyodbc.Error as ex:
            logger.debug("Didn't drop index on %s, error %s" % (column, ex))

    def create_index(self, column, table):
        command = "CREATE INDEX idx_%s ON %s (%s);" % (column, table, column)
        logger.debug(command)

        try:
            self.conn = pyodbc.connect(self.connection_string)
            self.cur = self.conn.cursor()
            self.cur.execute(command)
            self.conn.commit()
            self.cur.close()
            self.conn.close()
            logger.debug('Created index on (%s) %s' % (table, column))
        except pyodbc.Error as ex:
            logger.debug("Didn't create index on %s, error %s" % (column, ex))

    """
        Environment-related methods
    """

    def reset_indexes(self):
        logger.info('Reset Indexes')
        # FETCH INDEX NAMES
        self.conn = pyodbc.connect(self.connection_string)
        self.cur = self.conn.cursor()

        for table in self.tables.keys():
            self.cur.execute('SHOW INDEXES FROM %s;' % table)            
            index_names = list()

            for index in self.cur.fetchall():
                index_names.append(index[2])

            for index in index_names:
                if "idx_" in index:
                    self.cur.execute("DROP INDEX %s ON %s;" % (index, table))

        self.conn.commit()
        self.cur.close()
        self.conn.close()

        return True

    def get_table_name(self, column):
        tables = self.get_current_state()
        for tab in tables:
            for col in tables[tab]:
                if column == col:
                    return tab
    
    def apply_state(self, state, only_optimized=True):
        logger.info('Apply state to the database')
        for table, columns in state.items():
            for column, indexed in columns.items():
                if only_optimized:
                    if column not in self.tables[table]:
                        logger.debug(
                            f'Skipping {column} once we are not optimizing it.'
                        )
                        continue
                if indexed == 1:
                    self.create_index(column, table)
                elif indexed == 0:
                    self.drop_index(column, table)
    
    def apply_vector(self, vector):
        state = self.vector_to_state(vector)
        self.apply_state(state)                


if __name__ == '__main__':
    db = Database()
    # cstate = db.get_current_state()
    # c = db.state_to_vector(cstate)
    # print

    db.apply_vector(np.zeros(22))
    cstate = db.get_current_state()
    c = db.state_to_vector(cstate)
    print(c)
    
    # print("Vector of indexes: ", db.get_vector_of_indexes())    
    
    # print("Vector of indexes: ", db.get_vector_of_indexes())
    # print("Get Columns List: ", db.get_list_columns())
    # print("Size of Columns list: ", len(db.get_list_columns()))
    # print("Get Indexes List: ", db.get_list_indexes())
    # print("Size of Indexes list: ", len(db.get_list_indexes()))
    # print("Table name: ", db.get_table_name('l_orderkey'))
    # print("Get table indexed columns: ", db.get_table_indexed_columns())
    # print("Get indexes: ", db.get_current_state())