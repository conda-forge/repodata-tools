import hashlib


def compute_md5(pth):
    with open(pth, "rb") as f:
        file_hash = hashlib.md5()
        while True:
            chunk = f.read(8192)
            if not chunk:
                break
            file_hash.update(chunk)
    return file_hash.hexdigest()


def chunk_iterable(iterable, chunk_size):
    """Generate sequences of `chunk_size` elements from `iterable`.

    https://stackoverflow.com/a/12797249/1745538
    """
    chunk_size = max(chunk_size, 1)

    iterable = iter(iterable)
    while True:
        chunk = []
        try:
            for _ in range(chunk_size):
                chunk.append(next(iterable))
            yield chunk
        except StopIteration:
            if chunk:
                yield chunk
            break
