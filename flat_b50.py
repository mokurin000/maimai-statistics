import polars as pl

df = (
    pl.scan_parquet("b50.parquet")
    .select(pl.col("userId"), pl.col("userRating").struct.unnest())
    .drop(
        pl.col("nextRatingList"),
        pl.col("udemae"),
    )
    .select(
        pl.col("userId"),
        pl.col("rating"),
        pl.concat_list("ratingList", "newRatingList").alias("totalList"),
    )
    .explode("totalList")
    .select(
        pl.col("userId"),
        pl.col("rating"),
        pl.col("totalList").struct.unnest(),
    )
)

df.collect().write_parquet("b50_flat.parquet")
