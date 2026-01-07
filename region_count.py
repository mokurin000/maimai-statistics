import polars as pl

from pyecharts import options as opts
from pyecharts.charts import Map


def region_name(region_id: int) -> str:
    name = {
        1: "北京",
        2: "重庆",
        3: "上海",
        4: "天津",
        5: "安徽",
        6: "福建",
        7: "甘肃",
        8: "广东",
        9: "贵州",
        10: "海南",
        11: "河北",
        12: "黑龙江",
        13: "河南",
        14: "湖北",
        15: "湖南",
        16: "江苏",
        17: "江西",
        18: "吉林",
        19: "辽宁",
        20: "青海",
        21: "陕西",
        22: "山东",
        23: "山西",
        24: "四川",
        26: "云南",
        27: "浙江",
        28: "广西",
        29: "内蒙古",
        30: "宁夏",
        31: "新疆",
        32: "西藏",
    }.get(region_id, "未知")

    special = {
        "内蒙古": "自治区",
        "西藏": "自治区",
        "宁夏": "回族自治区",
        "广西": "壮族自治区",
        "新疆": "维吾尔自治区",
    }

    if name == "未知":
        return name

    if name in special:
        return f"{name}{special[name]}"
    elif name in ["北京", "上海", "重庆", "天津"]:
        return f"{name}市"
    else:
        return f"{name}省"


df = (
    pl.scan_parquet("regions.parquet")
    .sort("play_count", descending=True)
    .unique(subset=["user_id"], keep="first")
    .with_columns(pl.col("region_id").map_elements(region_name, return_dtype=pl.String))
    .group_by("region_id")
    .len()
    .sort("len", descending=True)
    .collect()
)

c = (
    Map(
        init_opts=opts.InitOpts(
            width="1600px",  # 设置图表宽度
            height="1000px",  # 设置图表高度
        )
    )
    .add("", list(df.iter_rows()), "china")
    .set_global_opts(
        title_opts=opts.TitleOpts(title="最常上机地区"),
        visualmap_opts=opts.VisualMapOpts(
            max_=80000,
        ),
    )
    .render("maimai-region-common.html")
)

df = (
    pl.scan_parquet("regions.parquet")
    .sort("created", descending=False)
    .unique(subset=["user_id"], keep="first")
    .with_columns(pl.col("region_id").map_elements(region_name, return_dtype=pl.String))
    .group_by("region_id")
    .len()
    .sort("len", descending=True)
    .collect()
)

c = (
    Map(
        init_opts=opts.InitOpts(
            width="1600px",  # 设置图表宽度
            height="1000px",  # 设置图表高度
        )
    )
    .add("", list(df.iter_rows()), "china")
    .set_global_opts(
        title_opts=opts.TitleOpts(title="最早上机地区"),
        visualmap_opts=opts.VisualMapOpts(
            max_=80000,
        ),
    )
    .render("maimai-region-first.html")
)
