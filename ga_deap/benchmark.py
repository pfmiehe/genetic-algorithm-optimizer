import logging
import os
import time
from collections import namedtuple
from multiprocessing import Process, Queue

import mysql

import config
import numpy as np
import utils
from database import Database
from scipy import stats  # FOR GEOMETRIC MEAN

logger = logging.getLogger(__name__)

Metrics = namedtuple(
    typename='Metrics',
    field_names=[
        'power',
        'throughput',
        'qphh',
        'db_size',        
        'time',
        'cost',
        'evaluation_time'
    ]
)


class Benchmark:
    
    def __init__(self, database):
        self.DB_CONFIG = utils.get_conn_dict()
        # IT IS NOT BEING USED
        self.db = database        

        '''
        SCALE_FACTOR    1   10  30  100
        NUM_STREAMS     2   3   4   5
        '''
        self.SCALE_FACTOR = 1
        self.NUM_STREAMS = 2

        '''
            Global configuration
        '''
        
        self.REFRESH_FILES_PATH = config.DBGEN

        '''
            Refresh stream sequence number (leave it at 1)
        '''
        self.refresh_stream_number = 1

        '''
            Keeps track of refresh files order by storing sequence number in a .txt file
            If the file does not exist, it will create and start from number 1
        '''

    def __get_refresh_stream_number(self):
        if os.path.exists("%s/refresh_stream_number.txt" % self.REFRESH_FILES_PATH):
            with open("%s/refresh_stream_number.txt" % self.REFRESH_FILES_PATH, "r+") as f:
                self.refresh_stream_number = int(f.read())
        else:
            with open("%s/refresh_stream_number.txt" % self.REFRESH_FILES_PATH, "w+") as f:
                f.write("%d" % self.refresh_stream_number)

    def __set_refresh_stream_number(self):
        with open("%s/refresh_stream_number.txt" % self.REFRESH_FILES_PATH, "w+") as f:
            f.write("%d" % self.refresh_stream_number)
        logging.debug('Set refresh stream number {}'.format(self.refresh_stream_number))
    '''
        Loads data from refresh files to temporary tables in the database
    '''

    def __load_refresh_stream_data(self):
        # print("*** Load refresh stream number:", self.refresh_stream_number)

        # STRINGS TO BE EXECUTED BY CURSOR
        delete = "load data local infile '{}/delete.{}' into table rfdelete fields terminated by '|' " \
                 "lines terminated by '\n';".format(self.REFRESH_FILES_PATH, self.refresh_stream_number)
        orders = "load data local infile '{}/orders.tbl.u{}' into table orders_temp fields terminated by '|' " \
                 "lines terminated by '\n';".format(self.REFRESH_FILES_PATH, self.refresh_stream_number)
        lineitem = "load data local infile '{}/lineitem.tbl.u{}' into table lineitem_temp fields terminated by '|' " \
                   "lines terminated by '\n';".format(self.REFRESH_FILES_PATH, self.refresh_stream_number)

        # OPEN DB CONNECTION, EXECUTE LOADS AND CLOSE CONNECTION
        conn = mysql.connector.connect(**self.DB_CONFIG)
        mysql.connector.connect()
        cursor = conn.cursor()
        cursor.execute(delete)
        cursor.execute(orders)
        cursor.execute(lineitem)
        conn.close()

        # INCREMENT REFRESH STREAM NUMBER FOR NEXT STREAM
        self.refresh_stream_number += 1

    '''
        Refresh functions calling respective procedures in the DB
        Each function returns its duration
    '''

    def __insert_refresh_function(self):
        # OPENS CONNECTION, EXECUTES PROCEDURE AND CLOSES CONNECTION
        conn = mysql.connector.connect(**self.DB_CONFIG)
        cursor = conn.cursor()
        cursor.execute("SET PROFILING = 1")
        cursor.callproc("INSERT_REFRESH_FUNCTION")
        cursor.execute("SHOW PROFILES")
        results = cursor.fetchall()
        conn.close()

        # SUM AND RETURN TOTAL EXECUTION TIME FROM FETCHED RESULTS
        duration = 0
        for row in results:
            duration += row[1]
        return duration

    def __delete_refresh_function(self):
        # OPENS CONNECTION, EXECUTES PROCEDURE AND CLOSES CONNECTION
        conn = mysql.connector.connect(**self.DB_CONFIG)
        cursor = conn.cursor()
        cursor.execute("SET PROFILING = 1")
        cursor.callproc("DELETE_REFRESH_FUNCTION")
        cursor.execute("SHOW PROFILES")
        results = cursor.fetchall()
        conn.close()

        # SUM AND RETURN TOTAL EXECUTION TIME FROM FETCHED RESULTS
        duration = 0
        for row in results:
            duration += row[1]
        return duration

    '''
        Refresh streams executed in parallel by process in throughput test
    '''

    def __run_refresh_streams(self, results_queue):
        # LIST FOR STORING REFRESH FUNCTIONS DURATION
        refresh_streams_duration = []

        # RUNS A NUMBER OF REFRESH STREAMS ACCORDING TO SCALE FACTOR
        for _ in range(self.NUM_STREAMS):
            self.__load_refresh_stream_data()
            refresh_streams_duration.append(self.__insert_refresh_function())
            refresh_streams_duration.append(self.__delete_refresh_function())

        # RETURNS TOTAL REFRESH STREAMS EXECUTION TIME
        results_queue.put(refresh_streams_duration)

    '''
        Query stream called from power test and throughput test (NUM_STREAMS in parallel)
    '''

    def __run_query_stream(self, results_queue):
        # START DB CONNECTION
        conn = mysql.connector.connect(**self.DB_CONFIG)
        cursor = conn.cursor()

        # SET PROFILING
        cursor.execute("SET PROFILING_HISTORY_SIZE = 22")
        cursor.execute("SET PROFILING = 1")

        # CALL QUERY STREAM PROCEDURE
        cursor.callproc("QUERY_STREAM")

        # SHOW PROFILES
        cursor.execute("SHOW PROFILES")

        # TRANSFORM FETCHED PROFILES INTO DICT OF (QUERY NUM: DURATION)
        profiles = dict()
        for row in cursor.fetchall():
            profiles[row[0]] = row[1]

        # CLOSE DB CONNECTION
        conn.close()

        # RETURN PROFILES RESULT
        results_queue.put(profiles)  # IF RUNNING IN A PROCESS
        return profiles

    def __run_power_test(self):
        logger.debug('Running power test')
        # LOAD REFRESH STREAM DATA
        self.__load_refresh_stream_data()

        # INSERT REFRESH FUNCTION
        insert_refresh_profile = self.__insert_refresh_function()

        # RUN QUERY STREAM
        query_stream_profiles = self.__run_query_stream(Queue())
        # print("*** Query stream duration:", sum(query_stream_profiles.values()))

        # DELETE REFRESH FUNCTION
        delete_refresh_profile = self.__delete_refresh_function()

        # CREATES LIST OF DUsRATIONS OF THE 22 QUERIES AND REFRESH FUNCTIONS
        power_test_profiles = list(query_stream_profiles.values())
        power_test_profiles.append(insert_refresh_profile)
        power_test_profiles.append(delete_refresh_profile)

        # CALCULATES GEOMETRIC MEAN
        geo_mean = stats.gmean(power_test_profiles)
        power = (3600 / geo_mean) * self.SCALE_FACTOR

        # RETURN POWER@SIZE METRIC
        return power

    '''
        Runs the whole throughput test, composed of # processes for # query streams and one process for # refresh streams
    '''

    def __run_throughput_test(self):
        logging.debug('Running throughput Test')
        # DECLARING PROCESSES
        results_queue = Queue()
        throughput_test_profiles = []
        streams = []
        for _ in range(self.NUM_STREAMS):
            streams.append(Process(target=self.__run_query_stream, args=(results_queue,)))
        streams.append(Process(target=self.__run_refresh_streams, args=(results_queue,)))

        # START TIMING EXECUTION
        start_time = time.time()

        # START PROCESSES
        for p in streams:
            p.start()

        for p in streams:
            throughput_test_profiles.append(results_queue.get())

        # JOIN PROCESSES
        for p in streams:
            p.join()

        # FINISH TIMING EXECUTION
        elapsed_time = time.time() - start_time

        return ((2 * 22) / elapsed_time) * 3600 * self.SCALE_FACTOR
    
    def get_cost(self):
        raise NotImplementedError('Not implemented yet.')

    def get_runtime(self):
        logging.debug('Getting workload runtime')
        # START DB CONNECTION
        conn = mysql.connector.connect(**self.DB_CONFIG)
        cursor = conn.cursor()

        # SET PROFILING
        cursor.execute("SET PROFILING_HISTORY_SIZE = 22")
        cursor.execute("SET PROFILING = 1")

        # CALL QUERY STREAM PROCEDURE
        cursor.callproc("QUERY_STREAM")

        # SHOW PROFILES
        cursor.execute("SHOW PROFILES")

        # TRANSFORM FETCHED PROFILES INTO DICT OF (QUERY NUM: DURATION)
        # profiles = dict()
        v = []
        for row in cursor.fetchall():
            # profiles[row[0]] = row[1]
            v.append(row[1])        

        result = np.sum(v)

        # CLOSE DB CONNECTION
        conn.close()

        # RETURN PROFILES RESULT
        return {'time': result}

    def get_storage_size(self):
        logging.debug('Getting storage size')
        # START DB CONNECTION
        conn = mysql.connector.connect(**self.DB_CONFIG)
        cursor = conn.cursor()

        # UPDATE STATISTICS
        operations = [
            'ANALYZE TABLE region;', 
            'ANALYZE TABLE nation;',
            'ANALYZE TABLE customer;',
            'ANALYZE TABLE orders;',                 
            'ANALYZE TABLE part;',
            'ANALYZE TABLE supplier;',
            'ANALYZE TABLE lineitem;',
            'ANALYZE TABLE partsupp;',
        ]
                
        for operation in operations:
            cursor.execute(operation)
            cursor.fetchall()
        
        # VERIFY THE DATABASE SIZE FOR ALL TABLES
        # cursor.execute("SHOW TABLE STATUS WHERE Row_format LIKE 'Dynamic'")
        cursor.execute("SELECT sum(round(stat_value*@@innodb_page_size/1024/1024, 2)) size_in_mb FROM mysql.innodb_index_stats WHERE stat_name = 'size' AND index_name != 'PRIMARY' and database_name='tpch';")
        index_size = float(np.array(list(cursor.fetchall())).sum())
        logging.debug(f'Database size: {index_size}')

        cursor.execute("SELECT sum(DATA_LENGTH)/1024/1024 from information_schema.tables where table_schema = 'tpch';")
        data_size = float(np.array(list(cursor.fetchall())).sum())
        logging.debug(f'Database size: {data_size}')
        # TRANSFORM FETCHED QUERY INTO DICT OF (TABLE: SIZE) AND A LIST SUM OF SIZES
        
        conn.close()

        return {
            'data_size': data_size, 
            'index_size': index_size
        }

    def get_qphh(self):
        '''
        Runs the whole power test, composed of: 
            (1) Insert RF; 
            (2) Query stream; 
            (3) Delete RF
        '''
        from timeit import default_timer as dt
        begin = dt()
        logging.debug('Run QPHH benchmark')
        # READ LAST REFRESH STREAM NUMBER
        self.__get_refresh_stream_number()

        power = self.__run_power_test()

        throughput = self.__run_throughput_test()
        
        logging.debug('Calculating qphh')
        qphh = np.sqrt(power * throughput)
                
        self.refresh_stream_number += self.NUM_STREAMS

        # WRITE LAST REFRESH STREAM NUMBER
        self.__set_refresh_stream_number()
        
        # Record time required to run the whole benchmark
        end = dt()
        benchmark_time = end-begin

        return {
            'power': power, 
            'throughput': throughput, 
            'qphh': qphh, 
            'benchmark_time': benchmark_time
        }


if __name__ == '__main__':
    benchmark = Benchmark()
    print(benchmark)
    
    # benchmark.db.apply_vector(np.ones(22))
    # bench = benchmark.get_storage_size()
    # print(bench)
