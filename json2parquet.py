import orjson as json
import polars as pl

from zstd import uncompress


def process(name: str):
    with open(f"{name}.json.zst", "rb") as f:
        bytes_zstd = f.read()
    uncompressed = uncompress(bytes_zstd)
    data = json.loads(uncompressed)
    pl.DataFrame(data).write_parquet(f"{name}.parquet")


if __name__ == "__main__":
    process("players")
