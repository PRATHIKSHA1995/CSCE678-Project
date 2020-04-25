import json
import boto3
import bokeh
import bokeh.palettes
from bokeh.plotting import figure
from bokeh.transform import dodge
from bokeh.embed import components
from bokeh.io import output_file, show
from datetime import datetime, timedelta
from flask import Flask, render_template
from bokeh.models import DatetimeTickFormatter
from boto3.dynamodb.conditions import Key, Attr
from bokeh.models import ColumnDataSource, NumeralTickFormatter, HoverTool, LinearColorMapper, ColorBar, BasicTicker, PrintfTickFormatter
from bokeh.palettes import Viridis256
from bokeh.transform import transform

app = Flask(__name__)

@app.route('/')
def analytics():
    uniqueDates = set()
    dynamodb = boto3.resource('dynamodb', region_name='us-west-1')
    table = dynamodb.Table('CSCE-678-Spark')
    response = table.query(KeyConditionExpression=Key('key').eq('historic-tweet-per-day'))["Items"][0]
    negativeTweets = json.loads(response["neg-tweet-dict"])
    positiveTweets = json.loads(response["pos-tweet-dict"])
    negativeTweetDates = set(negativeTweets.keys())
    positiveTweetDates = set(positiveTweets.keys())
    uniqueDates = list(negativeTweetDates.intersection(positiveTweets))
    dateRanges = set()
    for date in uniqueDates:
        dt = datetime.strptime(date, '%Y-%m-%d')
        start = dt - timedelta(days=dt.weekday())
        end = start + timedelta(days=6)
        start = start.strftime('%Y-%m-%d')
        end = end.strftime('%Y-%m-%d')
        range = str(start) + " : " + str(end)
        dateRanges.add(range)
    dateRanges = list(dateRanges)
    dateRanges.sort()
    data = {"dateRanges" : dateRanges, "Positive" : len(dateRanges) * [0], "Negative": len(dateRanges) * [0]}
    for date in uniqueDates:
        dt = datetime.strptime(date, '%Y-%m-%d')
        start = dt - timedelta(days=dt.weekday())
        end = start + timedelta(days=6)
        start = start.strftime('%Y-%m-%d')
        end = end.strftime('%Y-%m-%d')
        range = str(start) + " : " + str(end)
        data["Positive"][dateRanges.index(range)] += positiveTweets[date]
        data["Negative"][dateRanges.index(range)] += negativeTweets[date]
    source = ColumnDataSource(data=data)
    maxValue = max(max(data["Positive"]), max(data["Negative"]))
    p = figure(x_range=dateRanges, y_range=(0, int(1.1 * maxValue)), plot_height=600, plot_width=800, title="No. of Positive & Negative tweets by week", toolbar_location=None, tools="")
    p.vbar(x=dodge("dateRanges", -0.25, range=p.x_range), top="Positive", width=0.2, source=source, color="#718dbf", legend_label="Positive")
    p.vbar(x=dodge("dateRanges", 0.0, range=p.x_range), top="Negative", width=0.2, source=source, color="#e84d60", legend_label="Negative")
    p.x_range.range_padding = 0.1
    p.xgrid.grid_line_color = None
    p.legend.location = "top_left"
    p.legend.orientation = "horizontal"
    p.left[0].formatter.use_scientific = False
    p.yaxis.axis_label = "No. of tweets"
    p.xaxis.axis_label = "Week"
    p.yaxis[0].formatter = NumeralTickFormatter(format='0.0 a')
    p.add_tools(HoverTool(tooltips=[('Date Range', "@dateRanges"), ('No. of tweets', '$y{(0 a)}')], mode='vline'))
    script, div = components(p)

    uniqueDates.sort()
    data2 = {"uniqueDates" : uniqueDates, "Positive" : [], "Negative": []}
    colors = bokeh.palettes.d3['Category10'][10]
    for date in uniqueDates:
        data2["Positive"].append(positiveTweets[date])
        data2["Negative"].append(negativeTweets[date])
    maxValue = max(max(data2["Positive"]), max(data2["Negative"]))
    uniqueDates = [datetime.strptime(date, '%Y-%m-%d') for date in uniqueDates]
    p2 = figure(y_range=(0, int(1.1 * maxValue)), x_axis_type='datetime', plot_height=600, plot_width=800, title="No. of Positive & Negative tweets by day", toolbar_location=None, tools="")
    p2.line(uniqueDates, data2["Positive"], legend="Positive", line_color=colors[2], line_width = 3, alpha=0.8)
    p2.line(uniqueDates, data2["Negative"], legend="Negative", line_color=colors[1], line_width = 3, alpha=0.8)
    p2.legend.location = "top_left"
    p2.legend.orientation = "horizontal"
    p2.left[0].formatter.use_scientific = False
    p2.yaxis.axis_label = "No. of tweets"
    p2.xaxis.axis_label = "Date"
    p2.yaxis[0].formatter = NumeralTickFormatter(format='0 a')
    p2.xaxis.formatter = DatetimeTickFormatter(days=["%Y-%m-%d"])
    p2.add_tools(HoverTool(tooltips=[('Date', "$x{%F}"), ('No. of tweets', '$y{(0 a)}')], formatters={'$x': 'datetime'}, mode='vline'))
    script2, div2 = components(p2)

    response = table.query(KeyConditionExpression=Key('key').eq('history-topic-modelling-frequency-weight-sw'))["Items"][0]
    topics10 = json.loads(response["topics-10"])
    topics = ["1", "2", "3", "4", "5"]
    data3 = {"words": [], "weights": [], "frequencies": [], "scaled": [], "topic": []}
    for topic in topics:
        res = topics10[topic]
        for key in res:
            if key in data3["words"]:
                index = data3["words"].index(key)
                data3["weights"][index] = max(data3["weights"][index], res[key][1])
                data3["scaled"][index] = data3["weights"][index] * 2000
            else:
                data3["words"].append(key)
                data3["weights"].append(res[key][1])
                if res[key][0] is None or str(res[key][0]) == "null":
                    data3["frequencies"].append(0)
                else:
                    data3["frequencies"].append(res[key][0][0])
                data3["scaled"].append(res[key][1] * 2000)
                data3["topic"].append(str(topic))
    zipped = list(zip(data3["frequencies"], data3["weights"], data3["words"], data3["scaled"], data3["topic"]))
    zipped.sort()
    data3["frequencies"], data3["weights"], data3["words"], data3["scaled"], data3["topic"] = zip(*zipped)
    source = ColumnDataSource(data=data3)
    p3 = figure(x_range = (0, max(data3["frequencies"])), y_range = data3["words"], plot_height=600, plot_width=1600)
    color_mapper = LinearColorMapper(palette = Viridis256, low = min(data3["weights"]), high = max(data3["weights"]))
    color_bar = ColorBar(color_mapper = color_mapper, location = (0, 0),ticker = BasicTicker())
    p3.add_layout(color_bar, 'right')
    p3.scatter(x = 'frequencies', y = 'words', size = 'scaled', fill_color = transform('weights', color_mapper), source = source)
    p3.add_tools(HoverTool(tooltips = [('Topic', '@topic')]))
    p3.below[0].formatter.use_scientific = False
    script3, div3 = components(p3)
    print(print(bokeh.__version__))
    return render_template('index.html', sent_div=div, sent_script=script, sent_div2=div2, sent_script2=script2, sent_script3=script3, sent_div3=div3, data="")