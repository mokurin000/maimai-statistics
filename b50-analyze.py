from decimal import Decimal
from pathlib import Path

import polars as pl
from jinja2 import Template
from pykakasi.kakasi import Kakasi

KAKASI = Kakasi()
COLOR_MAP = {
    "EASY": "#66d85b",
    "ADVANCED": "#fed652",
    "EXPERT": "#fa6a7d",
    "MASTER": "#a147eb",
    "RE:MASTER": "#a777d6",
}
with open("template/b50.html.j2", "r", encoding="utf-8") as f:
    HTML_TEMPLATE: Template = Template(f.read())


def render_html(b50_rating: int, output: str | Path = "suggestion.html"):
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

    min_rating = b50_rating // 100 * 100

    if min_rating >= 15000:
        max_rating = min_rating + 100
    elif min_rating >= 13000:
        max_rating = min_rating + 100
    else:
        max_rating = min_rating + 200

    target_total = max_rating

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
        pl.col("player_rating").lt(max_rating),
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
                lambda level: [
                    "EASY",
                    "ADVANCED",
                    "EXPERT",
                    "MASTER",
                    "RE:MASTER",
                    "UTAGE",
                ][level],
                return_dtype=pl.String,
            )
        )
        # .filter(pl.col("pass_rate").ge(0.98))
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

    with open(output, "w", encoding="utf-8") as f:
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
    for rating in range(5000, 16401, 100):
        render_html(rating, f"suggestion-{rating:05}.html")
