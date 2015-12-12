import os

# TODO docu


class MapReduce(object):  # TODO abstract class
    """
    https://en.wikipedia.org/wiki/MapReduce

    [(k1, v1), ..., (kn, vn)] -> [(l1, w1), ..., (ln, wn)]
    """

    def __init__(self):
        pass

    def run(self):
        input_data = self.input()
        map_results = self._map_phase(input_data)
        shuffle_results = self._shuffle_phase(map_results)
        reduce_results = self._reduce_phase(shuffle_results)
        self.output(reduce_results)

    def input(self):
        raise NotImplementedError

    def output(self, data):
        raise NotImplementedError

    def _map_phase(self, data):
        """
        TODO

        :param data:
        :param data: list((key, value))
        :return: list((l1, x1))
        """
        return [i for key_value in data for i in self.map(key_value[0], key_value[1])]

    def map(self, key, value):
        """
        TODO

        :param key:
        :param value:
        :return: [(l1, x1), ..., (ln, xn)]
        """
        raise NotImplementedError

    @classmethod
    def _shuffle_phase(cls, data):
        shuffle_out = dict()
        for key_value in data:
            if key_value[0] not in shuffle_out:
                shuffle_out[key_value[0]] = list()
            shuffle_out[key_value[0]].append(key_value[1])
        return [(key, value) for key, value in shuffle_out.items()]

    def _reduce_phase(self, data):
        """
        TODO

        :param data:
        :type data: list((key, list(value)))
        :return:
        """
        return [(key_value[0], self.reduce(key_value[0], key_value[1])) for key_value in data]

    def reduce(self, key, value_list):
        """
        TODO

        :param key:
        :param value_list:
        :return: [w1, ..., wn]
        """
        raise NotImplementedError


class FileLineMapReduce(MapReduce):  # TODO abstract class

    def __init__(self, input_folder, output_folder):
        super().__init__()
        self._input_folder = input_folder
        self._output_folder = output_folder

    def input(self):
        output = list()

        folder = os.listdir(self._input_folder)
        for entry in folder:
            if not os.path.isfile(entry):
                continue

            with open(entry, 'r') as file:
                output.extend(self.read_file(file, entry))

        return output

    def read_file(self, file, fname):
        raise NotImplementedError

    def output(self, data):
        fname = os.path.join(self._output_folder, 'output.dat')
        with open(fname, 'w') as file:
            self.write_file(file, fname, data)

    def write_file(self, file, fname, data):
        raise NotImplementedError
