import io
import itertools
import multiprocessing

import psycopg2
import siphash
import progressbar

LATIN = 'abcdefghijklmnopqrstuvwxyz'
DIGITS = '0123456789'
ALPHABET = LATIN + LATIN.upper() + DIGITS
# keylen 4 is 2GB postgres data
KEYLEN = 5
SEED = b'1234567812345678'


def calculate(keys):
    csv = io.StringIO()
    for key in keys:
        csv.write('%s\t%s\n' % (key, siphash.siphash(key.encode(), SEED)))
    csv.seek(0)
    with psycopg2.connect('dbname=siphash user=postgres password=postgres host=127.0.0.1') as conn:
        with conn.cursor() as cur:
            cur.copy_from(csv, 'hashes', columns=('key', 'hash'))
        conn.commit()
    return len(keys)


def split_to_chunks(iterable, size=10):
    # https://stackoverflow.com/a/24527424
    iterator = iter(iterable)
    for first in iterator:
        yield itertools.chain([first], itertools.islice(iterator, size - 1))


def main():
    pool = multiprocessing.Pool(processes=16)
    with psycopg2.connect('dbname=siphash user=postgres password=postgres host=127.0.0.1') as conn:
        with conn.cursor() as cur:
            cur.execute("CREATE TABLE IF NOT EXISTS hashes(key text, hash text)")
        conn.commit()
        with progressbar.ProgressBar(max_value=len(ALPHABET) ** KEYLEN) as bar:
            keys = (''.join(x) for x in itertools.product(ALPHABET, repeat=KEYLEN))
            key_chunks = (list(x) for x in split_to_chunks(keys, 1_000_000))
            for results_num in pool.imap(calculate, key_chunks, chunksize=1):
                bar.update(results_num)


if __name__ == '__main__':
    main()
