def U8TO64_LE(p: bytes) -> int:
    return p[0] | p[1] << 8 | p[2] << 16 | p[3] << 24 | p[4] << 32 | p[5] << 40 | p[6] << 48 | p[7] << 56


assert U8TO64_LE(b'12345678') == 0x3837363534333231, hex(U8TO64_LE(b'12345678'))


def ROTL(x, b):
    return ((x << b) | (x >> (64 - b))) & (2 ** 64 - 1)


def SIPROUND(v0, v1, v2, v3):
    v0 += v1
    v0 &= (2 ** 64 - 1)
    v1 = ROTL(v1, 13)
    v1 ^= v0
    v1 &= (2 ** 64 - 1)
    v0 = ROTL(v0, 32)
    v2 += v3
    v2 &= (2 ** 64 - 1)
    v3 = ROTL(v3, 16)
    v3 ^= v2
    v3 &= (2 ** 64 - 1)
    v0 += v3
    v0 &= (2 ** 64 - 1)
    v3 = ROTL(v3, 21)
    v3 ^= v0
    v3 &= (2 ** 64 - 1)
    v2 += v1
    v2 &= (2 ** 64 - 1)
    v1 = ROTL(v1, 17)
    v1 ^= v2
    v1 &= (2 ** 64 - 1)
    v2 = ROTL(v2, 32)
    return v0, v1, v2, v3


def split_to_chunks(lst, n):
    """Yield successive n-sized chunks from items."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


def siphash(value: bytes, seed: bytes) -> int:
    v0 = 0x736f6d6570736575
    v1 = 0x646f72616e646f6d
    v2 = 0x6c7967656e657261
    v3 = 0x7465646279746573
    k0 = U8TO64_LE(seed[:8])
    k1 = U8TO64_LE(seed[8:])
    b = len(value) << 56
    v3 ^= k1
    v2 ^= k0
    v1 ^= k1
    v0 ^= k0
    chunks = list(split_to_chunks(value, 8))
    if len(chunks[-1]) != 8:
        left_chunk = chunks.pop()
    else:
        left_chunk = None
    for chunk in chunks:
        m = U8TO64_LE(chunk)
        v3 ^= m
        v0, v1, v2, v3 = SIPROUND(v0, v1, v2, v3)
        v0 ^= m

    if left_chunk:
        for i in range(len(left_chunk), 0, -1):
            chunk_byte_shift = (i - 1) * 8
            b |= (left_chunk[i - 1] << chunk_byte_shift)
    v3 ^= b
    v0, v1, v2, v3 = SIPROUND(v0, v1, v2, v3)

    v0 ^= b
    v2 ^= 0xff

    v0, v1, v2, v3 = SIPROUND(v0, v1, v2, v3)
    v0, v1, v2, v3 = SIPROUND(v0, v1, v2, v3)

    b = v0 ^ v1 ^ v2 ^ v3

    return b


assert siphash(b"hello world", b'1234567812345678') == 0x89cb5e38dae0f000, hex(siphash(b"hello world", b'1234567812345678'))
