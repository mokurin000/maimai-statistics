from decimal import Decimal

import polars as pl
from jinja2 import Template
from pykakasi.kakasi import Kakasi

pl.Config.set_tbl_rows(-1)
pl.Config.set_tbl_width_chars(-1)
pl.Config.set_tbl_formatting("UTF8_FULL")

KAKASI = Kakasi()
COLOR_MAP = {
    "绿": "#66d85b",
    "黄": "#fed652",
    "红": "#fa6a7d",
    "紫": "#a147eb",
    "白": "#a777d6",
}
with open("template/b50.html.j2", "r", encoding="utf-8") as f:
    HTML_TEMPLATE: Template = Template(f.read())


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

    if min_rating >= 13000:
        target_total = (1 + min_rating // 500) * 500
        max_rating = min_rating + 100
    else:
        target_total = (1 + min_rating // 1000) * 1000
        max_rating = min_rating + 200

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
        .select(
            [
                "level",
                "music_title",
                "difficulty",
                "pass_rate",
                "passed_players",
                "music_id",
            ]
        )
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
    print(suggestion.drop("music_id"))
    print("pass_rate: B50含该曲目难度中，达成率高于 nessacary 的占比")

    with open("suggestion.html", "w", encoding="utf-8") as f:
        suggestion = list(suggestion.iter_rows(named=True))
        for entry in suggestion:
            entry: dict[str, str]
            entry["color"] = COLOR_MAP[entry["level"]]

            music_id = entry["music_id"]
            entry["cover_pic"] = (
                f"https://jacket.maimai.realtvop.top/{music_id % 10000:05}.png"
            )

            title = entry["music_title"]
            difficulty: Decimal = entry["difficulty"]
            if (difficulty * 10) % 10 < 6:
                difficulty_category = f"{int(difficulty)}"
            else:
                difficulty_category = f"{int(difficulty)}+"
            if not title[0].isascii():
                title = KAKASI.convert(title)[0]["hira"]
            entry["hint"] = (
                f"{title[0]} / {difficulty_category} / {'DX' if entry['music_id'] > 10000 else 'SD'}"
            )
        f.write(
            HTML_TEMPLATE.render(
                data=suggestion, title=f"{min_rating}-{max_rating} 上分推荐"
            )
        )


if __name__ == "__main__":
    main()
