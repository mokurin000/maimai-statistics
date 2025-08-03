import json
from typing import Callable

from pyecharts import options as opts
from pyecharts.charts import Bar
from pyecharts.globals import ThemeType
import zstd


def access_playerrating(obj: dict) -> int:
    return obj["playerRating"]


def access_rating(obj: dict) -> int:
    return obj["userRating"]["rating"]


# 读取 ZSTD 文件
def load_data(file_path, access_rating=Callable[[dict], int]):
    with open(file_path, "rb") as f:
        raw = f.read()
    uncompress = zstd.decompress(raw)
    data = json.loads(uncompress)

    return [access_rating(obj) for obj in data]


# 绘制 playerRating 分布直方图
def create_histogram(
    data,
    start=0,
    end=16501,
    output_file="player_rating_distribution.html",
):
    # 设置分箱
    bin_width = 100
    bins = list(range(start, end, bin_width))

    # 统计每个分箱的频率
    hist_data = [0] * (len(bins) - 1)
    for rating in data:
        for i in range(len(bins) - 1):
            if bins[i] <= rating < bins[i + 1]:
                hist_data[i] += 1
                break

    # 创建直方图
    bar = (
        Bar(
            init_opts=opts.InitOpts(
                theme=ThemeType.LIGHT,
                width="1600px",  # 设置图表宽度
                height="1000px",  # 设置图表高度
            )
        )
        .add_xaxis([f"{bins[i]}-{bins[i + 1]}" for i in range(len(bins) - 1)])
        .add_yaxis("Count", hist_data)
        .set_global_opts(
            title_opts=opts.TitleOpts(title="Player Rating Distribution"),
            xaxis_opts=opts.AxisOpts(
                name="Player Rating", axislabel_opts=opts.LabelOpts(rotate=45)
            ),
            yaxis_opts=opts.AxisOpts(name="Count"),
            toolbox_opts=opts.ToolboxOpts(),
            datazoom_opts=[opts.DataZoomOpts()],
        )
    )

    # 渲染到 HTML 文件
    bar.render(output_file)
    return bar


# 主程序
if __name__ == "__main__":
    try:
        file_path = "players.json.zst"
        ratings = load_data(file_path, access_playerrating)
        create_histogram(ratings, end=10001, output_file="below-w0.html")
        create_histogram(ratings, start=10000, output_file="after-w0.html")

        file_path = "b50.json.zst"
        ratings = load_data(file_path, access_rating)
        create_histogram(ratings, end=10001, output_file="below-w0-b50.html")
        create_histogram(ratings, start=10000, output_file="after-w0-b50.html")
    except FileNotFoundError:
        print("Error: data.json file not found")
    except json.JSONDecodeError:
        print("Error: Invalid JSON format")
    except Exception as e:
        print(f"Error: {e.__class__, __name__}: {str(e)}")
