from glob import glob
import pandas as pd
from multiprocessing import cpu_count, Pool
from os.path import isdir, join, isfile
from functools import partial
from os import scandir
from collections.abc import Iterable
from sqlalchemy import (create_engine, engine, Column,
                        Text, Table, MetaData, String,
                        Boolean, DateTime, Integer,
                        Float, ForeignKey, CheckConstraint, PrimaryKeyConstraint)
from numpy import dtype
from subprocess import Popen, PIPE


class DataModel:

    def __init__(self, file_paths_or_dataframe, files_format=None, cpu=None, **kwargs):

        self.extension = files_format
        self.load_function = kwargs
        self.cores = cpu
        self.data = file_paths_or_dataframe

    @property
    def cores(self):

        return self._cpu

    @property
    def data(self):

        return self._data

    @property
    def load_function(self):

        return self._load_function

    @property
    def extension(self):

        return self._extension

    @cores.setter
    def cores(self, cpu):

        if not cpu:
            self._cpu = cpu_count()
        else:
            self._cpu = cpu

    @data.setter
    def data(self, files_path_or_data):

        if isinstance(files_path_or_data, (pd.DataFrame, pd.Series)):
            self._data = files_path_or_data
        else:
            if isinstance(files_path_or_data, str):
                if isdir(files_path_or_data):
                    self.input = glob(join(files_path_or_data, '*{}'.format(self.extension)))
                elif isfile(files_path_or_data):
                    self.input = [files_path_or_data, ]
            elif isinstance(files_path_or_data, (list, tuple)):
                self.input = files_path_or_data
            else:
                raise Exception('file_paths_or_dataframe format is not valid')
            if self.input:
                self._data = self.__get_data()

    @load_function.setter
    def load_function(self, kwargs):
        func = self.__get_function()
        if not func:
            self._load_function = None
        else:
            self._load_function = partial(self.func, function=partial(self.__get_function(), **kwargs))

    @extension.setter
    def extension(self, format_str):

        if format_str is None:
            self._extension = format_str
        elif format_str.lower() in ['json', 'csv', 'xlsx', 'feather', 'parquet']:
            self._extension = '.{}'.format(format_str)
        else:
            raise Exception('extension not valid should be: ')

    @staticmethod
    def func(item, function):

        try:
            temp_data = function(item)
            if isinstance(temp_data, pd.Series):
                temp_columns = temp_data.index
            else:
                temp_columns = temp_data.columns
            # print(item)
            return temp_columns.tolist(), temp_data.values
        except (pd.errors.ParserError, pd.errors.EmptyDataError, Exception) as error:
            # Logic to catch and log errors in a multiprocessing pool to be implemented
            # pass statement is currently deemed acceptable
            pass

    def __convert_to_df(func):

        def _wrapped_convert_to_df(self):

            output_list = func(self)
            output_list_filtered = [*self.filter_by_type(output_list, Iterable)]
            max_index = len(output_list_filtered)-1
            check_headers = True
            index = 0
            while check_headers:
                headers = output_list_filtered[index][0]
                if headers:
                    check_headers = False
                else:
                    index += 1
                if index == max_index and not output_list_filtered[index][0]:
                    raise Exception('No valid headers found')

            data_list = [data_values for columns, data_values
                         in output_list_filtered if columns == headers]
            return pd.DataFrame(data_list, columns=headers)

        return _wrapped_convert_to_df

    @staticmethod
    def filter_by_type(sequence, data_type):
        for element in sequence:
            if isinstance(element, data_type):
                yield element

    def __wrapper_parallel_load(func):

        def wrapped_parallel_load(self):
            variables, function, n_cpu = func(self)
            pool = Pool(n_cpu)
            multiprocess_output = pool.map(function, variables, n_cpu)
            pool.close()
            pool.join()
            return multiprocess_output

        return wrapped_parallel_load

    @__convert_to_df
    @__wrapper_parallel_load
    def __get_data(self):

        return self.input, self.load_function, self.cores

    def split_by(self, granularity):
        """
        Splits and remove duplicates from the loaded table based on a columns list
        :param granularity:
            Type: list
            A list of columns for keep
        :return: pandas DataFrame
        """
        return self.data[granularity].drop_duplicates().reset_index(drop=True)

    def __get_function(self):

        functions_dict = {'.json': pd.read_json,
                          '.csv': pd.read_csv,
                          '.xlsx': pd.read_excel,
                          '.feather': pd.read_feather,
                          '.parquet': pd.read_parquet}
        if not self.extension:
            return None
        else:
            return functions_dict.get(self.extension)


class DatabaseManagement:

    dtypes_dict = {'object': String,
                   'str': String,
                   'int64': Integer,
                   'int': Integer,
                   'float64': Float,
                   'float': Float,
                   'datetime64': DateTime,
                   'bool':	Boolean}

    def __init__(self, username, password, dialect,
                 host, port, database_name=None):

        self.url = engine.url.URL(**{'password': password,
                                        'username': username,
                                        'drivername': dialect,
                                        'host': host,
                                        'port': port,
                                        'database': database_name})
        self.engine = self.url

        self.meta = MetaData()

    @property
    def engine(self):

        return self._engine

    @engine.setter
    def engine(self, url):

        self._engine = create_engine(url)

    def to_sql_copy(self, table_name, from_table, columns_kwargs):

        if isinstance(from_table, pd.DataFrame):

            columns_dict_base = self.__get_parameters_dict(from_table.dtypes.to_dict(),
                                                           columns_kwargs)

            for key, item in columns_dict_base.items():
                columns_dict_base[key]['type'] = self.__pandas_to_sql_types(item['type'])

            selectable_columns = [self.__column_parameters(items) for items in columns_dict_base.items()]

            object_table = Table(table_name, self.meta, *selectable_columns)

            if not self.engine.dialect.has_table(self.engine, table_name):

                self.meta.create_all(self.engine)

            self.__copy_to_table(table_name, from_table, '#')

    def __copy_to_table(self, table_name, data, delimiter):

        self._open_process()
        statement = "\COPY {} from STDIN DELIMITER '{}';\n".format(table_name, delimiter)
        print(statement)
        # # COPY DATA IN NEWLY CREATED TABLE
        data = data.apply(lambda col: col.str.replace(delimiter, ' ') if col.dtype == 'object' else col, axis=0)
        data_str = data.to_csv(sep=delimiter, index=False, header=False).split('\n')[:-1]
        self.psql.stdin.write(bytes(statement, 'utf-8'))
        [self.psql.stdin.write(line) for line in map(lambda row: bytes(row+'\n', 'utf-8'), data_str)]
        self.psql.stdin.write(bytes("\.\n", 'utf-8'))

    @staticmethod
    def __get_parameters_dict(main_dict, additional_dict, level_1_key='type'):

        output_dict = dict(zip(main_dict.keys(),
                           zip(len(main_dict)*[level_1_key, ], main_dict.values())))

        output_dict = {key: [value, ] for key, value in output_dict.items()}

        for key in output_dict.keys():
            if key in additional_dict.keys():
                for item in additional_dict.get(key).items():
                    output_dict.get(key).append(item)

        output_dict_w_kwargs = {key: dict(value) for key, value in output_dict.items()}

        return output_dict_w_kwargs

    @staticmethod
    def __column_parameters(items):

        column_name, args = items
        column_type = args.get('type')
        del args['type']
        extra_constraints = []
        if 'CheckConstraint' in args.keys():
            extra_constraints.append(CheckConstraint(args['CheckConstraint']))
            del args['CheckConstraint']

        return Column(column_name, column_type, *extra_constraints, **args)

    def _open_process(self):

        self.psql = Popen(['psql', str(self.url)], stdout=PIPE, stdin=PIPE)

    def __pandas_to_sql_types(self, type_to_convert):

        if isinstance(type_to_convert, dtype):

            type_to_convert = type_to_convert

        elif isinstance(type_to_convert, type):

            type_to_convert = type_to_convert.__name__

        return self.dtypes_dict.get(str(type_to_convert), Text)

    def drop_table(self):
        pass


if __name__ == '__main__':
  
    # THIS IS A WIP SCRIPT USED TO MOVE DATA FROM INDIVIDUAL FILES
    # TO A POSTRES DATABASE IN THE CLOUD
    # data is on disk; few 100k files; each file represents a song

    paths = [i.path for idx, i in enumerate(scandir(r'/Users/********/PycharmProjects/udacity/data')) if idx < 500000]

    # data_for_upload = DataModel(paths, 'json', **{'typ': 'series'})

    # data has already been harvested and consolidated into a csv
    data_for_upload = DataModel(pd.read_csv('test.csv'))

    artists_data = data_for_upload.split_by(['artist_id', 'song_id', 'title', 'duration', 'year'])

    # Initializing the DatabaseNanagement class
    # db is hosted in by AWS RDS

    db = DatabaseManagement('********', '********',
                           'postgresql', '*******.cvrvhtkqtojs.us-east-2.rds.amazonaws.com',
                           5432, 'postgres')

    # COPY 50K RECORDS IN DB IN 5s
    # CREATES TABLE IF DOESNT EXIST

    db.to_sql_copy("songs_table", artists_data, columns_kwargs={'artist_id':{'primary_key':False},
                                                                'song_id':{'nullable':False},
                                                                'year':{'nullable': True}})

