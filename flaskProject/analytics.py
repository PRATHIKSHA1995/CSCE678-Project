import json
import boto3
import bokeh
from bokeh.plotting import figure
from bokeh.transform import dodge
from bokeh.embed import components
from bokeh.io import output_file, show
from datetime import datetime, timedelta
from flask import Flask, render_template
from boto3.dynamodb.conditions import Key, Attr
from bokeh.models import ColumnDataSource, NumeralTickFormatter, HoverTool

app = Flask(__name__)

@app.route('/')
def analytics():
    uniqueDates = set()
    dynamodb = boto3.resource('dynamodb', region_name='us-east-2')
    table = dynamodb.Table('spark_analytics')
    response = table.query(KeyConditionExpression=Key('key').eq('tweet-per-day'))["Items"][0]
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
    script, div = components(p)
    print(print(bokeh.__version__))
    return render_template('index.html', sent_div=div, sent_script=script, data="")