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
from bokeh.models.widgets import Tabs, Panel
from bokeh.models import DatetimeTickFormatter
from boto3.dynamodb.conditions import Key, Attr
from bokeh.models import ColumnDataSource, NumeralTickFormatter, HoverTool
from bokeh.plotting import show, figure
from bokeh.models import ColumnDataSource, LinearColorMapper, ColorBar, BasicTicker, PrintfTickFormatter, HoverTool
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

    negative_number = int(sum(data['Negative'])/1000)
    positive_number = int(sum(data['Positive'])/1000)
    total_number = int(negative_number + positive_number)

    source = ColumnDataSource(data=data)
    maxValue = max(max(data["Positive"]), max(data["Negative"]))
    p = figure(x_range=dateRanges, y_range=(0, int(1.1 * maxValue)), plot_height=400, plot_width=1300, sizing_mode = "scale_both", title="No. of Positive & Negative tweets by week", toolbar_location=None, tools="")
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
   
    uniqueDates.sort()
    data2 = {"uniqueDates" : uniqueDates, "Positive" : [], "Negative": []}
    colors = bokeh.palettes.d3['Category10'][10]
    for date in uniqueDates:
        data2["Positive"].append(positiveTweets[date])
        data2["Negative"].append(negativeTweets[date])
    maxValue = max(max(data2["Positive"]), max(data2["Negative"]))
    uniqueDates = [datetime.strptime(date, '%Y-%m-%d') for date in uniqueDates]
    p2 = figure(y_range=(0, int(1.1 * maxValue)), x_axis_type='datetime', plot_height=400, plot_width=1300, sizing_mode = "scale_both", title="No. of Positive & Negative tweets by day", toolbar_location=None, tools="")
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
    

    day_panel = Panel(child=p2, title='Tweets by Day')
    week_panel = Panel(child=p, title='Tweets by Week')
    tabs = Tabs(tabs=[day_panel, week_panel])

    script, div = components(tabs)

    #On New Data
    # ###########################################################################################################
    uniqueDates = set()
    response = table.query(KeyConditionExpression=Key('key').eq('sentiment-analysis'))["Items"]
    originalData = dict()
    for item in response:
        #item = json.loads(item)
        originalData[item["date"]] = [int(item["pos-tweet"]),int(item["neg-tweet"])]
    uniqueDates = list(originalData.keys())
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
        data["Positive"][dateRanges.index(range)] += originalData[date][0]
        data["Negative"][dateRanges.index(range)] += originalData[date][1]

    today_negative_number = int(sum(data['Negative'])/1000)
    today_positive_number = int(sum(data['Positive'])/1000)
    today_total_number = int(negative_number + positive_number)

    source = ColumnDataSource(data=data)
    maxValue = max(max(data["Positive"]), max(data["Negative"]))
    p = figure(x_range=dateRanges, y_range=(0, int(1.1 * maxValue)), plot_height=400, plot_width=1300, sizing_mode = "scale_both", title="No. of Positive & Negative tweets by week", toolbar_location=None, tools="")
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

    uniqueDates.sort()
    data2 = {"uniqueDates" : uniqueDates, "Positive" : [], "Negative": []}
    colors = bokeh.palettes.d3['Category10'][10]
    for date in uniqueDates:
        data2["Positive"].append(originalData[date][0])
        data2["Negative"].append(originalData[date][1])
    maxValue = max(max(data2["Positive"]), max(data2["Negative"]))
    source = ColumnDataSource(data=data2)
    if len(uniqueDates):
        p2 = figure(x_range=uniqueDates, y_range=(0, int(1.1 * maxValue)),  plot_height=400, plot_width=1300, sizing_mode = "scale_both", title="No. of Positive & Negative tweets by day", toolbar_location=None, tools="")
    else:
        uniqueDates = [datetime.strptime(date, '%Y-%m-%d') for date in uniqueDates]
        p2 = figure(y_range=(0, int(1.1 * maxValue)), x_axis_type='datetime', plot_height=600, plot_width=800, sizing_mode = "scale_both", title="No. of Positive & Negative tweets by day", toolbar_location=None, tools="")
        p2.xaxis.formatter = DatetimeTickFormatter(days=["%Y-%m-%d"])
    p2.line(x="uniqueDates", y="Positive", legend_label="Positive", line_color=colors[2], line_width = 3, alpha=0.8, source=source)
    p2.line(x="uniqueDates", y="Negative", legend_label="Negative", line_color=colors[1], line_width = 3, alpha=0.8, source=source)
    p2.legend.location = "top_left"
    p2.legend.orientation = "horizontal"
    p2.left[0].formatter.use_scientific = False
    p2.yaxis.axis_label = "No. of tweets"
    p2.xaxis.axis_label = "Date"
    p2.yaxis[0].formatter = NumeralTickFormatter(format='0 a')
    p2.add_tools(HoverTool(tooltips=[('Date', "@uniqueDates"), ('No. of tweets', '$y{(0 a)}')], formatters={'$x': 'datetime'}, mode='vline'))


    day_panel = Panel(child=p2, title='Tweets by Day')
    week_panel = Panel(child=p, title='Tweets by Week')
    tabs = Tabs(tabs=[day_panel, week_panel])

    script2, div2 = components(tabs)





    print(print(bokeh.__version__))

    return render_template('index.html', sent_div1=div, sent_script1=script, sent_div2=div2, sent_script2=script2,total=total_number,pos=positive_number,neg=negative_number, today=today_total_number)

@app.route('/tagcloud')
def tagcloud():
   
    
    dynamodb = boto3.resource('dynamodb', region_name='us-west-1')
    table = dynamodb.Table('CSCE-678-Spark')
    # response = table.query(KeyConditionExpression=Key('key').eq('history-topic-modelling-frequency-weight-sw'))["Items"][0]
    response = table.query(KeyConditionExpression=Key('key').eq('topic-modelling'), ScanIndexForward=False)["Items"][0]

    topics10 = json.loads(response["topics-10"])

    topics_100 = topics10["0"]
    topics_101 = topics10["1"]
    topics_102 = topics10["2"]
    topics_103 = topics10["3"]
    topics_104 = topics10["4"]
    topics_105 = topics10["5"]
    topics_106 = topics10["6"]
    topics_107 = topics10["7"]
    topics_108 = topics10["8"]
    topics_109 = topics10["9"]

    x_list1 =[]
    for key in topics_100:
        dic = {}
        dic["text"] = key
        dic["weight"] = int((topics_100[key][1])*10000)
        x_list1.append(dic)

    x_list2 =[]
    for key in topics_101:
        dic = {}
        dic["text"] = key
        dic["weight"] = int((topics_101[key][1])*10000)
        x_list2.append(dic)

    x_list3 =[]
    for key in topics_102:
        dic = {}
        dic["text"] = key
        dic["weight"] = int((topics_102[key][1])*10000)
        x_list3.append(dic)

    x_list4 =[]
    for key in topics_103:
        dic = {}
        dic["text"] = key
        dic["weight"] = int((topics_103[key][1])*10000)
        x_list4.append(dic)

    x_list5 =[]
    for key in topics_104:
        dic = {}
        dic["text"] = key
        dic["weight"] = int((topics_104[key][1])*10000)
        x_list5.append(dic)

    x_list6 =[]
    for key in topics_105:
        dic = {}
        dic["text"] = key
        dic["weight"] = int((topics_105[key][1])*10000)
        x_list6.append(dic) 

    x_list7 =[]
    for key in topics_106:
        dic = {}
        dic["text"] = key
        dic["weight"] = int((topics_106[key][1])*10000)
        x_list7.append(dic)

    x_list8 =[]
    for key in topics_107:
        dic = {}
        dic["text"] = key
        dic["weight"] = int((topics_107[key][1])*10000)
        x_list8.append(dic)

    x_list9 =[]
    for key in topics_108:
        dic = {}
        dic["text"] = key
        dic["weight"] = int((topics_108[key][1])*10000)
        x_list9.append(dic)

    x_list10 =[]
    for key in topics_109:
        dic = {}
        dic["text"] = key
        dic["weight"] = int((topics_109[key][1])*10000)
        x_list10.append(dic)

    final = []
    final.append(x_list1)
    final.append(x_list2)
    final.append(x_list3)
    final.append(x_list4)
    final.append(x_list5)
    final.append(x_list6)
    final.append(x_list7)
    final.append(x_list8)
    final.append(x_list9)
    final.append(x_list10)

    return render_template('tagcloud.html', datacloud1=json.dumps(final))


@app.route('/tweetmap')
def tweetmap():
    
    
    
    dynamodb = boto3.resource('dynamodb', region_name='us-west-1')
    table = dynamodb.Table('CSCE-678-Spark')
    # response = table.query(KeyConditionExpression=Key('key').eq('historic-page-rank-results'))["Items"][0]
    response = table.query(KeyConditionExpression=Key('key').eq('page-rank-results'), ScanIndexForward=False)["Items"][0]


    users = json.loads(response['users'])
    user_names = list(users.keys())
    user_profiles = list(users.values())

    return render_template('tweetmap.html',user_names=user_names,user_profiles=user_profiles)

@app.route('/topic')
def topic():

    
    dynamodb = boto3.resource('dynamodb', region_name='us-west-1')
    table = dynamodb.Table('CSCE-678-Spark')
    # response = table.query(KeyConditionExpression=Key('key').eq('history-topic-modelling-frequency-weight-sw'))["Items"][0]
    response = table.query(KeyConditionExpression=Key('key').eq('topic-modelling'), ScanIndexForward=False)["Items"][0]

    topics10 = json.loads(response["topics-10"])
    
    topics = topics10.keys()
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
    p3 = figure(x_range = (0, max(data3["frequencies"])), y_range = data3["words"], plot_height=600,plot_width=1300, sizing_mode = "scale_both")
    color_mapper = LinearColorMapper(palette = Viridis256, low = min(data3["weights"]), high = max(data3["weights"]))
    color_bar = ColorBar(color_mapper = color_mapper, location = (0, 0),ticker = BasicTicker())
    p3.add_layout(color_bar, 'right')
    p3.scatter(x = 'frequencies', y = 'words', size = 'scaled', fill_color = transform('weights', color_mapper), source = source)
    p3.add_tools(HoverTool(tooltips = [('Topic', '@topic'), ('Word', '@words'), ('Frequency', '@frequencies')]))
    p3.below[0].formatter.use_scientific = False
    script3, div3 = components(p3)
    

    return render_template('topic.html',sent_script=script3,sent_div=div3)
