"""
Basic MapReduce implementation.

see: https://en.wikipedia.org/wiki/MapReduce
"""

import abc
import datetime
import os

__author__ = 'Christof Pieloth'


class MapReduce(abc.ABC):
    """
    Simple MapReduce implementation without concurrency.
    Input:  [(key_1, value_1), ..., (key_n, value_n)]
    Output: [(key'_1, value'_1), ..., (key'_m, value'_m)]
    """

    def __init__(self):
        pass

    @abc.abstractmethod
    def input(self):
        """
        Returns or retrieves the input data.

        :return: A list of key-value tuples, [(key, value), ...].
        """
        pass

    @abc.abstractmethod
    def output(self, data):
        """
        Handles the output, e.g. prints or writes it to a file.

        :param data: A list of key-value tuples, [(key, value), ...].
        """
        pass

    @abc.abstractmethod
    def map(self, key, value):
        """
        User implementation of the map function.

        :param key: The key to process.
        :param value: The key-related value to process.
        :return: A list of key-value tuples, [(key, value), ...].
        """
        pass

    @abc.abstractmethod
    def reduce(self, key, value_list):
        """
        User implementation of the reduce function.

        :param key: The key to process.
        :param value_list: A list of key-related values to process.
        :return: A list of values, [value, ...].
        """
        pass

    def run(self):
        """
        Starts the MapReduce implementation:
        input -> map -> shuffle -> reduce -> output.
        """
        input_data = self.input()
        map_results = self._map_phase(input_data)
        shuffle_results = self._shuffle_phase(map_results)
        reduce_results = self._reduce_phase(shuffle_results)
        self.output(reduce_results)

    def _map_phase(self, data):
        """
        Sequential implementation of the map phase.

        :param data: A list of key-value tuples, [(key, value), ...].
        :return: A list of key-value tuples, [(key, value), ...].
        """
        return [i for key_value in data for i in self.map(key_value[0], key_value[1])]

    @classmethod
    def _shuffle_phase(cls, data):
        """
        Implementation of the shuffle phase. Collects all values for equal keys.

        :param data: A list of key-value tuples, [(key, value), ...].
        :return: A list of key-values tuples, [(key, [value, ...]), ...].
        """
        shuffle_out = dict()
        for key_value in data:
            if key_value[0] not in shuffle_out:
                shuffle_out[key_value[0]] = list()
            shuffle_out[key_value[0]].append(key_value[1])
        return [(key, value) for key, value in shuffle_out.items()]

    def _reduce_phase(self, data):
        """
        Sequential implementation of the reduce phase.

        :param data: A list of key-values tuples, [(key, [value, ...]), ...].
        :return: A list of key-values tuples, [(key, [value, ...]), ...].
        """
        return [(key_value[0], self.reduce(key_value[0], key_value[1])) for key_value in data]


class FileLineMapReduce(MapReduce):
    """

    """

    def __init__(self, input_folder, output_folder):
        super().__init__()
        self._input_folder = input_folder
        self._output_folder = output_folder

    @abc.abstractmethod
    def read_file(self, file, fname):
        """
        Reads/parses the file content.

        :param file: An open file handle.
        :param fname: File name.
        :return: A list of key-value tuples, [(key, value), ...].
        """
        pass

    @abc.abstractmethod
    def write_file(self, file, fname, data):
        """
        Writes/serializes the data.

        :param file: An open file handle.
        :param fname: File name.
        :param data: A list of key-value tuples, [(key, value), ...].
        """
        pass

    def input(self):
        """
        Reads content of all files from the given directory, not recursively.

        :return: A list of key-value tuples, [(key, value), ...].
        """
        output = list()

        folder = os.listdir(self._input_folder)
        for fname in folder:
            if not os.path.isfile(fname):
                continue

            with open(fname, 'r') as file:
                output.extend(self.read_file(file, fname))

        return output

    def output(self, data):
        """
        Writes the data to the output directory: YYYYMMDDThhmmss_output.dat

        :param data: A list of key-value tuples, [(key, value), ...].
        """
        now = datetime.datetime.now()

        fname = os.path.join(self._output_folder, now.strftime("%Y%m%dT%H%M%S") + '_output ' + '.dat')
        with open(fname, 'w') as file:
            self.write_file(file, fname, data)
