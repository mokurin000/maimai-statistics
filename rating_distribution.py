import json
from pyecharts import options as opts
from pyecharts.charts import Bar
from pyecharts.globals import ThemeType


# 读取 JSON 文件
def load_data(file_path):
    with open(file_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    return [item["playerRating"] for item in data]


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
    file_path = "players.json"
    try:
        ratings = load_data(file_path)
        create_histogram(ratings, end=10001, output_file="below-w0.html")
        create_histogram(ratings, start=10000, output_file="after-w0.html")
        print("Histogram has been generated as player_rating_distribution.html")
    except FileNotFoundError:
        print("Error: data.json file not found")
    except json.JSONDecodeError:
        print("Error: Invalid JSON format")
    except Exception as e:
        print(f"Error: {str(e)}")
