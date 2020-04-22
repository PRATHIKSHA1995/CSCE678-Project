from flask import Flask, render_template
import boto3
from boto3.dynamodb.conditions import Key, Attr
import json
from bokeh.io import output_file, show
from bokeh.models import ColumnDataSource, NumeralTickFormatter, HoverTool
from bokeh.plotting import figure
from bokeh.transform import dodge
from bokeh.embed import components
import bokeh
import json
from datetime import datetime, timedelta
from math import pi

app = Flask(__name__)

@app.route('/')
def analytics():
    uniqueDates = set()
    data = r'{"key": "tweet-per-day", "neg-tweet-dict": "{\"2020-01-01\": 1, \"2020-03-04\": 4002, \"2020-03-05\": 8565, \"2020-03-06\": 6041, \"2020-03-07\": 4289, \"2020-03-08\": 4646, \"2020-03-09\": 21571, \"2020-03-10\": 57852, \"2020-03-11\": 88582, \"2020-03-12\": 158964, \"2020-03-13\": 313172, \"2020-03-14\": 136906, \"2020-03-15\": 144542, \"2020-03-16\": 183077, \"2020-03-17\": 245444, \"2020-03-18\": 186269, \"2020-03-19\": 247309, \"2020-03-20\": 224201, \"2020-03-22\": 207665, \"2020-03-23\": 206541, \"2020-03-24\": 192129, \"2020-03-25\": 199055, \"2020-03-26\": 263159, \"2020-03-27\": 200620, \"2020-03-28\": 166696}", "pos-tweet-dict": "{\"2020-03-04\": 2181, \"2020-03-05\": 4631, \"2020-03-06\": 3185, \"2020-03-07\": 2156, \"2020-03-08\": 2234, \"2020-03-09\": 11927, \"2020-03-10\": 38074, \"2020-03-11\": 59874, \"2020-03-12\": 100749, \"2020-03-13\": 216857, \"2020-03-14\": 104157, \"2020-03-15\": 108065, \"2020-03-16\": 137122, \"2020-03-17\": 191355, \"2020-03-18\": 138963, \"2020-03-19\": 196065, \"2020-03-20\": 182670, \"2020-03-22\": 181811, \"2020-03-23\": 166500, \"2020-03-24\": 152874, \"2020-03-25\": 175823, \"2020-03-26\": 224450, \"2020-03-27\": 167803, \"2020-03-28\": 133945}"}'
    response = json.loads(data)
    negativeTweets = json.loads(response["neg-tweet-dict"])
    positiveTweets = json.loads(response["pos-tweet-dict"])
    negativeTweetDates = set(negativeTweets.keys())
    positiveTweetDates = set(positiveTweets.keys())
    uniqueDates = list(negativeTweetDates.intersection(positiveTweets))
    dateRanges = set()
    # data = {"uniqueDates" : uniqueDates, "Positive" : [], "Negative": []}
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
    p = figure(x_range=dateRanges, y_range=(0, max(max(data["Positive"]), max(data["Negative"]))), plot_height=600, plot_width=900, title="No. of Positive & Negative tweets by week", toolbar_location=None, tools="")
    p.vbar(x=dodge("dateRanges", -0.25, range=p.x_range), top="Positive", width=0.2, source=source, color="#718dbf", legend_label="Positive")
    p.vbar(x=dodge('dateRanges',  0.0,  range=p.x_range), top="Negative", width=0.2, source=source, color="#e84d60", legend_label="Negative")
    p.x_range.range_padding = 0.1
    p.xgrid.grid_line_color = None
    p.legend.location = "top_left"
    p.legend.orientation = "horizontal"
    p.left[0].formatter.use_scientific = False
    p.yaxis.axis_label = "Number of tweets"
    p.xaxis.axis_label = "Week"
    p.yaxis[0].formatter = NumeralTickFormatter(format='0.0 a')
    # dynamodb = boto3.resource('dynamodb', region_name='us-east-2')
    # table = dynamodb.Table('test')
    # response = table.scan()
    # print(response)
    # return render_template('index.html', data=response)
    script, div = components(p)
    print(print(bokeh.__version__))
    print("okay")
    return render_template('index.html', sent_div=div, sent_script=script, data="")