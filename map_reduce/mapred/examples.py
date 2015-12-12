import mapred.map_reduce as mr


class WordCount(mr.MapReduce):
    def __init__(self):
        super().__init__()

    def input(self):
        return [('file1', 'foo bar baz baz foo'),
                ('file2', 'foo bar foo baz bar'),
                ('file3', 'foo bar baz baz foo')]

    def output(self, data):
        print('%s' % data)

    def map(self, key, value):
        return [(w.strip(), 1) for w in value.split(' ')]

    def reduce(self, key, value_list):
        return [sum(value_list)]

    @classmethod
    def main(cls):
        mr = cls()
        mr.run()


class WordCountFolder(mr.FileLineMapReduce):

    def __init__(self, input_folder, output_folder):
        super().__init__(input_folder, output_folder)
        self._wc = WordCount()

    def read_file(self, file, fname):
        output = list()
        for line in file:
            output.append((fname, line))
        return output

    def write_file(self, file, fname, data):
        for d in data:
            file.write('%s: %d\n' % (d[0], d[1].pop()))

    def map(self, key, value):
        return self._wc.map(key, value)

    def reduce(self, key, value_list):
        return self._wc.reduce(key, value_list)

    @classmethod
    def main(cls):
        mr = cls('.', '.')
        mr.run()


if __name__ == "__main__":
    WordCount.main()
