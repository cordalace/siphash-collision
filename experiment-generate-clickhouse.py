import itertools
import multiprocessing
import concurrent.futures

from clickhouse_driver import Client
import siphash

LATIN = 'abcdefghijklmnopqrstuvwxyz'
DIGITS = '0123456789'
ALPHABET = LATIN + LATIN.upper() + DIGITS
# keylen 4 is 2GB postgres data
KEYLEN = 5
SEED = b'1234567812345678'
PARTITIONS = 10000


def insert_partition(rows):
    assert isinstance(rows, list), rows
    assert isinstance(rows[0], tuple), rows[0]
    assert len(rows[0]) == 2, rows[0]
    client = Client('localhost')
    try:
        client.execute('INSERT INTO hashes (data, hash) VALUES', rows)
    except Exception:
        client.disconnect()


def calculate(keys):
    partitions = {}
    for key in keys:
        hash_ = siphash.siphash(key.encode(), SEED)
        partitions.setdefault(hash_ % 5000, []).append((key, hash_))
    with concurrent.futures.ThreadPoolExecutor(max_workers=6) as executor:
        executor.map(insert_partition, list(partitions.values()))
    return len(keys)


def split_to_chunks(iterable, size=10):
    # https://stackoverflow.com/a/24527424
    iterator = iter(iterable)
    for first in iterator:
        yield itertools.chain([first], itertools.islice(iterator, size - 1))


def main():
    pool = multiprocessing.Pool(processes=16)
    client = Client('localhost')
    client.execute(
        (
            'CREATE TABLE IF NOT EXISTS hashes (data String, hash UInt64) '
            'ENGINE = ReplacingMergeTree() '
            'ORDER BY data '
            'PRIMARY KEY data '
            'PARTITION BY hash % {partitions}'
        ).format(partitions=PARTITIONS),
    )
    total = len(ALPHABET) ** KEYLEN
    keys = (''.join(x) for x in itertools.product(ALPHABET, repeat=KEYLEN))
    key_chunks = (list(x) for x in split_to_chunks(keys, 1_000_000))
    passed = 0
    for results_num in pool.imap(calculate, key_chunks, chunksize=1):
        passed += results_num
        print('Passed %s' % (100.0 * passed / total))


if __name__ == '__main__':
    main()
