import csv
import progressbar

DATA_DIR = '/mnt/volume_ams3_01/files'
PARTITIONS = 10000
EXPECTED_ROWS = 62 ** 5


def scan_for_duplicates(filename):
    with open(filename) as csvfile:
        reader = csv.reader(csvfile, delimiter=',')
        header = next(reader)
        assert header == ['data', 'hash'], header
        seen_hashes = set()
        for row in reader:
            hash_ = row[1]
            if hash_ in seen_hashes:
                raise RuntimeError('Hash %s duplicate' % hash_)
            seen_hashes.add(hash_)
            yield 1

def main():
    writers = {}
    with progressbar.ProgressBar(max_value=EXPECTED_ROWS) as bar:
        i = 0
        for partition in range(PARTITIONS):
            filename = DATA_DIR + '/partition-{}.csv'.format(partition)
            for one in scan_for_duplicates(filename):
                i += one
                bar.update(i)


if __name__ == '__main__':
    main()
