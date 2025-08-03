import polars as pl


def main():
    b50 = pl.scan_parquet("b50_flat.parquet")

    min_rating = int(input("请输入您目前的底分: "))
    min_rating = min_rating // 100 * 100
    max_rating = min_rating + 100

    if min_rating >= 13000:
        target_total = (1 + min_rating // 500) * 500
    else:
        target_total = (1 + min_rating // 1000) * 1000

    print(f"目标 Rating: {target_total}")

    target_rating = target_total // 50

    print(
        f"正在为您查找 [{min_rating}, {max_rating}] 区间中，\n常见 rating >= {target_rating} 的歌曲..."
    )

    filtered = b50.filter(
        pl.col("rating") >= min_rating,
        pl.col("rating") <= max_rating,
    )

    total = (
        filtered.group_by(
            pl.col("musicTitle"),
            pl.col("difficulty"),
            pl.col("level"),
        )
        .len()
        .rename({"len": "total_len"})
    )

    passed = (
        filtered.filter(
            pl.col("dxRating") >= target_rating,
        )
        .group_by(
            pl.col("musicTitle"),
            pl.col("difficulty"),
            pl.col("level"),
        )
        .len()
        .join(total, ["musicTitle", "level", "difficulty"])
        .with_columns((pl.col("len") / pl.col("total_len")).alias("pass_rate"))
        .sort("len", descending=True)
        # .filter(pl.col("len") >= 10)
        .filter(pl.col("pass_rate") < 1)
        .with_columns(
            pl.col("level").map_elements(
                lambda level: "绿黄红紫白宴"[level], return_dtype=pl.String
            )
        )
        .with_columns(
            pl.col("pass_rate").mul(100).round(2).cast(pl.String).add("%"),
        )
        .head(30)
        .select(["level", "difficulty", "pass_rate", "musicTitle", "len"])
        .rename({"musicTitle": "title", "len": "passed_players"})
        .collect()
    )
    pl.Config.set_tbl_rows(-1)
    pl.Config.set_tbl_width_chars(-1)
    print(passed)


if __name__ == "__main__":
    main()
