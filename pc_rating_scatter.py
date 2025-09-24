from typing import Literal

import polars as pl
import pyecharts.options as opts
from pyecharts.charts import Scatter


pc = (
    pl.scan_parquet("regions.parquet")
    .drop(["region_id", "created"])
    .group_by("user_id")
    .sum()
)

rating = (
    pl.scan_parquet("players.parquet")
    .select(["user_id", "player_rating"])
    .join(pc, on="user_id")
    .drop("user_id")
    .collect()
)


x_data = rating["play_count"]
y_data = rating["player_rating"]


def init_chart(
    x_type: Literal["value", "log"], x_min: int = 1, x_max: int = 5000
) -> Scatter:
    return (
        Scatter(
            init_opts=opts.InitOpts(
                width="1600px",  # 设置图表宽度
                height="1000px",  # 设置图表高度
            )
        )
        .set_series_opts()
        .set_global_opts(
            xaxis_opts=opts.AxisOpts(
                type_=x_type,
                min_=x_min,
                max_=x_max,
            ),
            yaxis_opts=opts.AxisOpts(
                type_="value",
                axistick_opts=opts.AxisTickOpts(is_show=True),
                splitline_opts=opts.SplitLineOpts(is_show=True),
            ),
            tooltip_opts=opts.TooltipOpts(is_show=False),
            visualmap_opts=opts.VisualMapOpts(max_=16400),
        )
        .add_xaxis(xaxis_data=x_data.to_list())
        .add_yaxis(
            series_name="",
            y_axis=y_data.to_list(),
            symbol_size=2.5,
            label_opts=opts.LabelOpts(is_show=False),
        )
    )


init_chart("value", 1).render("pc-rating-linear.html")
init_chart("log", 10, 10000).render("pc-rating-log.html")
