import csv
import progressbar

DATA_DIR = '/mnt/volume_ams3_01/files'
THRESHOLD = 1000
PARTITIONS = 10000
EXPECTED_ROWS = 62 ** 5


class PartitionWriter:

    def __init__(self, partition):
        self.partition = partition
        self.filename = DATA_DIR + '/partition-{}.csv'.format(self.partition)
        self.cache = []
        with open(self.filename, 'wb') as f:
            f.write(b'data,hash\n')

    def send(self, item):
        self.cache.append('{data},{hash}'.format(data=item[0], hash=item[1]))
        if len(self.cache) > THRESHOLD:
            self._flush()

    def _flush(self):
        block = ('\n'.join(self.cache) + '\n').encode()
        with open(self.filename, 'ab') as f:
            f.write(block)
        self.cache.clear()

    def close(self):
        self._flush()


def main():
    writers = {}
    for i in range(PARTITIONS):
        writers[i] = PartitionWriter(i)
    with open(DATA_DIR + '/huge.csv') as csvfile:
        reader = csv.reader(csvfile, delimiter=',')
        header = next(reader)
        assert header == ['data', 'hash'], header
        with progressbar.ProgressBar(max_value=EXPECTED_ROWS) as bar:
            i = 0
            for row in reader:
                data, hash_ = row[0], int(row[1])
                writers[hash_ % PARTITIONS].send((data, hash_))
                bar.update(i)
                i += 1
    for writer in writers.values():
        writer.close()


if __name__ == '__main__':
    main()
