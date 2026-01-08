import polars as pl

REGION_MAP = {
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
}

df = pl.scan_parquet("regions.parquet").with_columns(
    pl.col("region_id").cast(pl.String).replace(REGION_MAP).alias("region")
)

first = df.sort("created").unique("user_id", keep="first").select(["user_id", "region"])
common = (
    df.sort("play_count", descending=True)
    .unique("user_id", keep="first")
    .select(["user_id", "region"])
)

pl.Config.set_tbl_formatting("ASCII_FULL")
pl.Config.set_tbl_rows(-1)

print(
    first.join(common, on="user_id", how="inner", suffix="_common")
    .filter(pl.col("region").ne(pl.col("region_common")))
    .collect()
)
