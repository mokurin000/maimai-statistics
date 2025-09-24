import polars as pl


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
        .sort("passed_players", descending=True)
        .head(30)
    )
    pl.Config.set_tbl_rows(-1)
    pl.Config.set_tbl_width_chars(-1)
    pl.Config.set_tbl_formatting("UTF8_FULL")
    print(passed.rename({"passed_players": "passed"}))
    print("passed: B50含有该曲目难度的玩家中有多少已达到所需达成率")
    print("pass_rate: passed 相比于B50含该曲目难度的总人数占比")


if __name__ == "__main__":
    main()
