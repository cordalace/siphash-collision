import itertools
import multiprocessing

import progressbar
import siphash

LATIN = 'abcdefghijklmnopqrstuvwxyz'
DIGITS = '0123456789'
ALPHABET = LATIN + LATIN.upper() + DIGITS
# keylen 5 requires 23GB data
# keylen 6 requires 1.4TB data
KEYLEN = 5
SEED = b'1234567812345678'
PARTITIONS = 10000
PROCESSES = 16
DATA_DIR = '/mnt/volume_ams3_01/files'
# DATA_DIR = '/tmp/files'


def calculate(keys):
    text = '\n'.join('{},{}'.format(key, siphash.siphash(key.encode(), SEED)) for key in keys)
    block = (text + '\n').encode()
    return block, len(keys)


def split_to_chunks(iterable, size=10):
    # https://stackoverflow.com/a/24527424
    iterator = iter(iterable)
    for first in iterator:
        yield itertools.chain([first], itertools.islice(iterator, size - 1))


def main():
    pool = multiprocessing.Pool(processes=PROCESSES)
    keys = (''.join(x) for x in itertools.product(ALPHABET, repeat=KEYLEN))
    key_chunks = (list(x) for x in split_to_chunks(keys, 5_000_000))
    passed = 0
    with open(DATA_DIR + '/huge.csv', 'wb') as f:
        f.write(b'data,hash\n')
        with progressbar.ProgressBar(max_value=len(ALPHABET) ** KEYLEN) as bar:
            i = 0
            for result in pool.imap(calculate, key_chunks, chunksize=1):
                block, results_num = result
                f.write(block)
                passed += results_num
                i += results_num
                bar.update(i)


if __name__ == '__main__':
    main()
