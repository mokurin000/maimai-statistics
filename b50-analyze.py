from decimal import Decimal
import polars as pl

pl.Config.set_tbl_rows(-1)
pl.Config.set_tbl_width_chars(-1)
pl.Config.set_tbl_formatting("UTF8_FULL")


def main():
    musics = (
        pl.scan_parquet("musics.parquet")
        .select(["id", "name", "level", "difficulty"])
        .rename({"id": "music_id", "name": "music_title"})
    )
    players = pl.scan_parquet("players.parquet").select(
        [
            "user_id",
            "player_rating",
        ]
    )
    b50 = pl.scan_parquet("b50.parquet").join(players, on="user_id")

    min_rating = int(input("请输入您目前的底分: "))
    min_rating = min_rating // 100 * 100
    max_rating = min_rating + 500

    if min_rating >= 13000:
        target_total = (1 + min_rating // 500) * 500
    else:
        target_total = (1 + min_rating // 1000) * 1000

    print(f"目标 Rating: {target_total}")

    target_rating = target_total // 50
    rating = (
        pl.scan_csv("rating_table.csv")
        .filter(pl.col("target_rating").eq(target_rating))
        .collect()
    )

    print(
        f"正在为您查找 [{min_rating}, {max_rating}] 区间玩家中，\n常见 rating >= {target_rating} 的歌曲..."
    )

    filtered = b50.filter(
        pl.col("player_rating").ge(min_rating),
        pl.col("player_rating").le(max_rating),
    )

    total = filtered.group_by(
        pl.col("music_id"),
        pl.col("level"),
    ).len("total_player")

    passed = (
        filtered.filter(
            pl.col("achievement").ge(970000),
            pl.col("dx_rating") >= target_rating,
        )
        .group_by(
            pl.col("music_id"),
            pl.col("level"),
        )
        .len(name="passed_players")
        .join(total, ["music_id", "level"])
        .with_columns(
            (pl.col("passed_players") / pl.col("total_player")).alias("pass_rate")
        )
        .filter(pl.col("pass_rate") < 1)
        .join(musics.unique(), on=["music_id", "level"])
        .with_columns(
            pl.col("level").map_elements(
                lambda level: "绿黄红紫白宴"[level], return_dtype=pl.String
            )
        )
        .with_columns(
            pl.col("pass_rate").mul(100).round(2).cast(pl.String).add("%"),
        )
        .lazy()
        .select(["level", "music_title", "difficulty", "pass_rate", "passed_players"])
        .collect()
        .sort("passed_players", "pass_rate", descending=True)
        .drop("passed_players")
        .head(30)
    )

    def minimum_acc(difficulty: Decimal) -> str:
        acc = (
            rating.filter(pl.col("difficulty") <= difficulty)
            .get_column("achievement")
            .cast(pl.String)
            .first()
        )
        return f"{acc[:-4]}.{acc[-4:]}%"

    suggestion = passed.with_columns(
        pl.col("difficulty")
        .map_elements(minimum_acc, return_dtype=pl.String)
        .alias("nessacary")
    )
    print(suggestion)
    suggestion.to_pandas().to_html("suggestion.html", index=False)
    print("pass_rate: B50含该曲目难度中，达成率高于 nessacary 的占比")


if __name__ == "__main__":
    main()
