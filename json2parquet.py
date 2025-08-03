import orjson as json
import polars as pl

from zstd import uncompress


def main():
    with open("b50.json.zst", "rb") as f:
        bytes_zstd = f.read()
    uncompressed = uncompress(bytes_zstd)
    data = json.loads(uncompressed)
    pl.DataFrame(data).write_parquet("b50.parquet")


if __name__ == "__main__":
    main()
