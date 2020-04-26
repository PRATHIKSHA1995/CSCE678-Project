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
    # tweetsdata = r'{ "key": "sentiment-analysis", "neg-tweet": 3471444,"pos-tweet": 2703493,"total-tweet": 6174937 }'
    # response_tweet = json.loads(tweetsdata)
    # negative_number = int((response_tweet["neg-tweet"])/1000)
    # positive_number = int(response_tweet["pos-tweet"]/1000)
    # total_number = int(response_tweet["total-tweet"]/1000)

    uniqueDates = set()
    
    #dynamo historic-data

    dynamodb = boto3.resource('dynamodb', region_name='us-west-1')
    table = dynamodb.Table('CSCE-678-Spark')
    response = table.query(KeyConditionExpression=Key('key').eq('historic-tweet-per-day'))["Items"][0]

    #hard code
    # data = r'{"key": "tweet-per-day", "neg-tweet-dict": "{\"2020-01-01\": 1, \"2020-03-04\": 4002, \"2020-03-05\": 8565, \"2020-03-06\": 6041, \"2020-03-07\": 4289, \"2020-03-08\": 4646, \"2020-03-09\": 21571, \"2020-03-10\": 57852, \"2020-03-11\": 88582, \"2020-03-12\": 158964, \"2020-03-13\": 313172, \"2020-03-14\": 136906, \"2020-03-15\": 144542, \"2020-03-16\": 183077, \"2020-03-17\": 245444, \"2020-03-18\": 186269, \"2020-03-19\": 247309, \"2020-03-20\": 224201, \"2020-03-22\": 207665, \"2020-03-23\": 206541, \"2020-03-24\": 192129, \"2020-03-25\": 199055, \"2020-03-26\": 263159, \"2020-03-27\": 200620, \"2020-03-28\": 166696}", "pos-tweet-dict": "{\"2020-03-04\": 2181, \"2020-03-05\": 4631, \"2020-03-06\": 3185, \"2020-03-07\": 2156, \"2020-03-08\": 2234, \"2020-03-09\": 11927, \"2020-03-10\": 38074, \"2020-03-11\": 59874, \"2020-03-12\": 100749, \"2020-03-13\": 216857, \"2020-03-14\": 104157, \"2020-03-15\": 108065, \"2020-03-16\": 137122, \"2020-03-17\": 191355, \"2020-03-18\": 138963, \"2020-03-19\": 196065, \"2020-03-20\": 182670, \"2020-03-22\": 181811, \"2020-03-23\": 166500, \"2020-03-24\": 152874, \"2020-03-25\": 175823, \"2020-03-26\": 224450, \"2020-03-27\": 167803, \"2020-03-28\": 133945}"}'
    # response = json.loads(data)
    #

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
    p = figure(x_range=dateRanges, y_range=(0, int(1.1 * maxValue)), plot_height=400, plot_width=800, title="No. of Positive & Negative tweets by week", toolbar_location=None, tools="")
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
    
    # script, div = components(p)
    

    uniqueDates.sort()
    data2 = {"uniqueDates" : uniqueDates, "Positive" : [], "Negative": []}
    colors = bokeh.palettes.d3['Category10'][10]
    for date in uniqueDates:
        data2["Positive"].append(positiveTweets[date])
        data2["Negative"].append(negativeTweets[date])
    maxValue = max(max(data2["Positive"]), max(data2["Negative"]))
    uniqueDates = [datetime.strptime(date, '%Y-%m-%d') for date in uniqueDates]
    p2 = figure(y_range=(0, int(1.1 * maxValue)), x_axis_type='datetime', plot_height=400, plot_width=800, title="No. of Positive & Negative tweets by day", toolbar_location=None, tools="")
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
    
    # script2, div2 = components(p2)
   


    day_panel = Panel(child=p2, title='Tweets by Day')
    week_panel = Panel(child=p, title='Tweets by Week')
    tabs = Tabs(tabs=[day_panel, week_panel])

    script, div = components(tabs)

    print(print(bokeh.__version__))
    # return render_template('index.html', sent_div=div, sent_script=script, sent_div2=div2, sent_script2=script2, data="")
    return render_template('index.html', sent_div=div, sent_script=script, data=trial_data,total=total_number,pos=positive_number,neg=negative_number)

@app.route('/tagcloud')
def tagcloud():
   

    # data3 = r'{ "key": "topic-modelling-weight","topics-10": "{\"0\": {\"have\": 0.01221643911012684, \"your\": 0.00885795649364033, \"will\": 0.008688222042160078, \"people\": 0.008021710689365238, \"&amp;\": 0.0075534472371661995, \"from\": 0.007035480415355444, \"they\": 0.006993945256040651, \"about\": 0.006342564699413624, \"just\": 0.005984527299909026, \"what\": 0.005756468461952981}, \"1\": {\"from\": 0.00998083581394041, \"&amp;\": 0.00881910131295745, \"will\": 0.0075691910084945534, \"#coronavirusoutbreak\": 0.00725309882453103, \"have\": 0.004564686120523675, \"health\": 0.004506690984685253, \"#coronaviruspandemic\": 0.0042047417153742, \"help\": 0.0040585271190841715, \"spread\": 0.0038702471425600678, \"coronavirus\": 0.003790780235817288}, \"2\": {\"#coronaviruspandemic\": 0.011159605497120091, \"your\": 0.010588233258641897, \"about\": 0.008389224994369586, \"from\": 0.0057452901803035335, \"will\": 0.005394556584011442, \"have\": 0.004750875316316521, \"take\": 0.004116788506239247, \"please\": 0.004043851812768955, \"people\": 0.003966757650897105, \"during\": 0.0036431884913324113}, \"3\": {\"your\": 0.012805997968363275, \"coronavirus\": 0.007377280107542053, \"from\": 0.006110196165629589, \"#coronavirusoutbreak\": 0.00602383996137531, \"help\": 0.005671590945762276, \"stay\": 0.005080679380373325, \"delivered\": 0.005007891990266066, \"test\": 0.004991877815985675, \"support\": 0.004990203422037789, \"last\": 0.004861281702694061}, \"4\": {\"from\": 0.012306571474935676, \"what\": 0.010528956085522819, \"will\": 0.009284862444311951, \"some\": 0.005009477524828233, \"people\": 0.004790936011254813, \"working\": 0.004031479958422693, \"&amp;\": 0.0035777139631744622, \"more\": 0.003525466431066327, \"when\": 0.0034784774015154973, \"home\": 0.0034055024885506005}, \"5\": {\"from\": 0.008465705537215219, \"protect\": 0.006297115177994306, \"during\": 0.006246129811633153, \"will\": 0.006138076119550942, \"health\": 0.006137193874209476, \"help\": 0.005655659438179383, \"your\": 0.005291833054570317, \"&amp;\": 0.00519724723359971, \"need\": 0.005156879762602108, \"they\": 0.004794202299168345}, \"6\": {\"&amp;\": 0.005662876242522839, \"just\": 0.005518284506697296, \"coronavirus\": 0.003304060228341534, \"your\": 0.0032043179889105176, \"trump\": 0.003146007118253158, \"from\": 0.003116176342720684, \"positive\": 0.002705265699191609, \"prince\": 0.0026330267914525195, \"stay\": 0.002358896116213406, \"stock\": 0.0022055495413638403}, \"7\": {\"during\": 0.019678003723807635, \"these\": 0.017090097976253722, \"they\": 0.016739404562124752, \"trying\": 0.01446676832157127, \"italy\": 0.0142142172307333, \"share\": 0.013882512868585386, \"people\": 0.013221253400396692, \"friends\": 0.01307059156281985, \"your\": 0.01293580462243123, \"times.\": 0.012754824089559831}, \"8\": {\"cases\": 0.01825928979187887, \"from\": 0.012729469252257325, \"coronavirus\": 0.00895476455179408, \"total\": 0.006587402849292471, \"confirmed\": 0.006249461682652039, \"positive\": 0.005415686635763814, \"have\": 0.005394850942401742, \"#coronaviruspandemic\": 0.005365759566625595, \"deaths\": 0.005043017512574802, \"first\": 0.00419352313510375}, \"9\": {\"#coronaviruspandemic\": 0.012527311206543877, \"#coronavirusoutbreak\": 0.010167756058613314, \"#coronaoutbreak\": 0.009772170631304162, \"#coronavirusupdate\": 0.007752654986997216, \"#coronavirusupdates\": 0.004259623519816358, \"deaths\": 0.003875271829108196, \"cases\": 0.003725193282519284, \"#coronalockdown\": 0.0036058937974563785, \"#covid\": 0.0035772195365389, \"#corona\": 0.003424135250031158}}","topics-20": "{\"0\": {\"your\": 0.013763806669972768, \"stay\": 0.012908007926519707, \"have\": 0.010161726703075854, \"&amp;\": 0.00750362544063307, \"home\": 0.0060665063688476175, \"just\": 0.005433592262068221, \"from\": 0.0053436374894893935, \"#coronavirusoutbreak\": 0.005159631888344617, \"#coronaviruspandemic\": 0.005086179018769031, \"safe\": 0.005060793668232082}, \"1\": {\"from\": 0.010290516358983981, \"&amp;\": 0.009255053998004985, \"#coronavirusoutbreak\": 0.00860038882797595, \"will\": 0.005442510961434791, \"#coronaviruspandemic\": 0.005169976186341683, \"coronavirus\": 0.004852046950192663, \"fight\": 0.004351695262046226, \"health\": 0.004190219531465948, \"cases\": 0.004176841372622771, \"against\": 0.003855817210165973}, \"2\": {\"#coronaviruspandemic\": 0.014389624820145634, \"your\": 0.0064849370079586195, \"from\": 0.004683442055926344, \"about\": 0.004650569823314064, \"have\": 0.004315294174710219, \"will\": 0.00393462715079552, \"corona\": 0.0034311285578696124, \"#corona\": 0.003407987944434076, \"world\": 0.003240436388093107, \"#coronaviruslockdown\": 0.0030985063506212183}, \"3\": {\"your\": 0.015618441314058176, \"delivered\": 0.009146284018236129, \"i\\u2019ll\": 0.00835751387355024, \"deliver\": 0.008146641277478396, \"officials\": 0.007942915912761807, \"last\": 0.007895657537861924, \"copy\": 0.007633651235926757, \"signing\": 0.00750296390203188, \"support\": 0.007441797627161079, \"too:\": 0.0068382263373209014}, \"4\": {\"from\": 0.0058425612650268395, \"will\": 0.004500837254288443, \"what\": 0.0034447986638887545, \"people\": 0.002862635218672292, \"&amp;\": 0.002148782944677693, \"latest\": 0.0017746522699032857, \"some\": 0.001752331232229831, \"death\": 0.0017317760726416878, \"have\": 0.0017242680248416187, \"more\": 0.0016938457511925776}, \"5\": {\"from\": 0.004453234247324762, \"will\": 0.004206392879986252, \"help\": 0.003648125682655904, \"protect\": 0.002741717696874284, \"need\": 0.0025519594595240585, \"world\": 0.0024791545585596845, \"&amp;\": 0.0023637596283618578, \"they\": 0.0021728128917783483, \"have\": 0.0021419611336272936, \"health\": 0.0019566951701632273}, \"6\": {\"prince\": 0.004607856894732385, \"&amp;\": 0.003969787585566118, \"positive\": 0.0036431800447578443, \"charles\": 0.003448951016424357, \"just\": 0.0028179545137181055, \"from\": 0.002518554821714404, \"tests\": 0.0023530089637683943, \"trump\": 0.002277974703130425, \"your\": 0.0021962208705399826, \"tested\": 0.002172723341588384}, \"7\": {\"these\": 0.022431119725492044, \"times.\": 0.02216197210028969, \"friends\": 0.02177868831457439, \"during\": 0.021705331611493014, \"stand\": 0.02147870562904749, \"share\": 0.021007292333917727, \"italian\": 0.020746696225978592, \"trying\": 0.020325529657154583, \"italy\": 0.019668939288734453, \"they\": 0.019142730641709092}, \"8\": {\"cases\": 0.01892232317314769, \"from\": 0.012166640735223649, \"coronavirus\": 0.009712659333135398, \"#coronaviruspandemic\": 0.008478928013168031, \"positive\": 0.007860493014813305, \"total\": 0.007256941598326907, \"confirmed\": 0.006852358656679414, \"deaths\": 0.005372315068008297, \"have\": 0.004999447371790661, \"first\": 0.004841656004849317}, \"9\": {\"myths\": 0.0032513827867378074, \"about\": 0.0027804446051294964, \"deaths\": 0.002426255106372312, \"#coronaviruspandemic\": 0.00234173980173016, \"have\": 0.0021248284288251772, \"cases\": 0.0018742078290603617, \"#coronaoutbreak\": 0.0015786563396148492, \"facts\": 0.0014697403787089088, \"#coronavirusoutbreak\": 0.001172355744873261, \"&amp;\": 0.0011646031771817245}, \"10\": {\"will\": 0.009821735432760654, \"have\": 0.009659093908222241, \"people\": 0.00890926754603037, \"from\": 0.008874614353795513, \"about\": 0.007565815378451999, \"&amp;\": 0.00663293799005282, \"they\": 0.006073372649691082, \"what\": 0.006050957695413501, \"more\": 0.005285772686117359, \"just\": 0.005030691159059944}, \"11\": {\"they\": 0.0046096285314012365, \"have\": 0.004440589225099148, \"people\": 0.0034170399380377805, \"social\": 0.003069500061728061, \"&amp;\": 0.003011559940515011, \"their\": 0.0018980659761515019, \"what\": 0.00180078077644892, \"these\": 0.0017734158192782565, \"about\": 0.0016762365804204994, \"#coronavirusoutbreak\": 0.0016426771863825547}, \"12\": {\"will\": 0.0046121834218178885, \"@drtedros\": 0.0038756568511192266, \"@who\": 0.0037849338719105558, \"#coronavirusoutbreak\": 0.0035548005629553476, \"beat\": 0.00341091599850607, \"urgently\": 0.003345914445694119, \"million\": 0.002833477896949904, \"pandemic\": 0.00276135475397393, \"keep\": 0.002737410659441799, \"safe.\": 0.0027175410532171386}, \"13\": {\"just\": 0.0023050980516676474, \"have\": 0.0019016423723825424, \"&amp;\": 0.0017904239294589056, \"from\": 0.00160214850929706, \"#coronavirusoutbreak\": 0.0015868675363513822, \"more\": 0.0014732264097127232, \"people\": 0.0014429104063701345, \"going\": 0.0013652498948583149, \"will\": 0.0013572492930255943, \"stimulate\": 0.0013335293730517167}, \"14\": {\"your\": 0.01556806899841259, \"what\": 0.008421164802812806, \"people\": 0.007891960986192235, \"#coronaviruspandemic\": 0.007036688582037648, \"doing\": 0.005047289513316408, \"just\": 0.004955462823593105, \"time\": 0.004925416855319191, \"#stayathome\": 0.004840868669981878, \"have\": 0.003938710143507638, \"like\": 0.0038817562217738745}, \"15\": {\"from\": 0.0029923141699867957, \"they\": 0.0026869089200975893, \"about\": 0.0024345602897809433, \"coronavirus\": 0.002357589550570559, \"your\": 0.002349835322884764, \"will\": 0.0023168458248279855, \"&amp;\": 0.002284653179964038, \"have\": 0.0022668321560781793, \"#coronaviruspandemic\": 0.001631975343463783, \"#coronavirusoutbreak\": 0.0013963544002995273}, \"16\": {\"their\": 0.0032577886399721154, \"they\": 0.0020846084514077982, \"people\": 0.0017559983142435814, \"about\": 0.0017065371919551128, \"&amp;\": 0.0016712145715568482, \"from\": 0.0012005818980149732, \"during\": 0.001180282988159229, \"have\": 0.001163256021199236, \"will\": 0.0010544403287663382, \"#coronavirusexpert\": 0.0010190611444000075}, \"17\": {\"from\": 0.0036768555178993776, \"&amp;\": 0.002290178242079892, \"have\": 0.0022817985809458377, \"your\": 0.0018309753019746576, \"more\": 0.0014106903808901816, \"will\": 0.0014082512724828457, \"case\": 0.0013008828940490973, \"just\": 0.0012889188537090065, \"coronavirus\": 0.0012099900015348846, \"virus\": 0.0011084629585896167}, \"18\": {\"#coronaviruspandemic\": 0.006864215374185541, \"listen\": 0.005013970179096588, \"fight\": 0.0032624872587706973, \"best\": 0.003008479524370869, \"&amp;\": 0.002924025198540736, \"great\": 0.0024447930071224383, \"about\": 0.002175462927697085, \"music\": 0.001978111594624229, \"from\": 0.0018655714674031226, \"than\": 0.0017075453658888138}, \"19\": {\"your\": 0.01090801253465902, \"have\": 0.009918876628070263, \"during\": 0.007487690198169544, \"their\": 0.007233922817939553, \"from\": 0.0069357748206918335, \"help\": 0.006866178337122549, \"&amp;\": 0.006213858414985917, \"they\": 0.006100734223831845, \"#coronavirusoutbreak\": 0.005313358377397525, \"need\": 0.004822029249679881}}" }'
    
    dynamodb = boto3.resource('dynamodb', region_name='us-west-1')
    table = dynamodb.Table('CSCE-678-Spark')
    response = table.query(KeyConditionExpression=Key('key').eq('history-topic-modelling-frequency-weight-sw'))["Items"][0]
    topics10 = json.loads(response["topics-10"])

    topics_100 = topics10["0"]
    x_list =[]
    for key in topics_100:
        dic = {}
        dic["text"] = key
        dic["weight"] = int((topics_100[key][1])*1000)
        x_list.append(dic)


    # words_json = [{'text': word, 'weight': int(count*1000)} for word, count in topics_100.items()]  

    trial_data = json.dumps(x_list)
    return render_template('tagcloud.html', datacloud=trial_data)


@app.route('/tweetmap')
def tweetmap():
    # toptweets = r'{ "key": "page-rank-results","users": "{\"GlobalCalgary\": \"Your source for breaking news from #yyc and beyond. Story idea? Email calgary@globalnews.ca\", \"AdamMacVicar\": \"Journalist at @GlobalCalgary | I like to cover politics | Calgary by way of Labrador City | Rumble, Young Man, Rumble | Email: adam.macvicar@globalnews.ca\", \"mmusJu\": \"Leftwing internationalist. #GTTO #DumpTrump #ProEU #Staysafe #NHS #PPE #Test\", \"AsaadHannaa\": \"EmissaLLC CEO, int-Affairs fellowship at @ColumbiaSIPA\\nFormer #WhiteHelmets,  journalist @Almonitor @GVinArabic Human rights defender, Social media expert\", \"mterr337\": \"Ive been on top of the world & Ive been in the depths of the sea. Im back on top & the view is much better from here.\\n\\n\\nNo DMs.\\n\\n#JoeKnowsMe\", \"NikolovScience\": \"Ph.D. Physical Scientist with a broad range of interests in various fields of science, i.e. climate, cosmology, astrophysics, nutrition, archaeology etc.\", \"Jaecoby_\": \"Code: Jaecoby \\u2022 Turn Notis On \\u2022 (Daily Giveaways) \\u2022(Dm For Promos) \\ud83d\\udcb0\\u270c\\ufe0f\\u2022 $Jaecoby\", \"ABC7\": \"Your #1 breaking news source in Southern California. When you witness news, share it with #abc7eyewitness.\", \"DrThomasPaul\": \"Past Life Regression Center\\u00ae Founder, Therapist, Author and Activist.\\nPlease read Past Lives Therapy https://t.co/aRAwLSewC1 #pastliferegression #pastlivestherapy\", \"thesunilgarg\": \"#Sunil_Garg_ #Nifty #Banknifty #Sensex #Stock #Commodity #Currency #Broker strong focus on #Research , #Wealth Management& #Investments electronic&printmedia\"}"}'
    
    
    dynamodb = boto3.resource('dynamodb', region_name='us-west-1')
    table = dynamodb.Table('CSCE-678-Spark')
    response = table.query(KeyConditionExpression=Key('key').eq('historic-page-rank-results'))["Items"][0]

    users = json.loads(response['users'])
    user_names = list(users.keys())
    user_profiles = list(users.values())

    return render_template('tweetmap.html',user_names=user_names,user_profiles=user_profiles)

@app.route('/topic')
def topic():

    # topicdata = r'{ "key": "topic-modelling-frequency-weight","topics-10": "{\"5\": {\"from\": [[763136], 0.008465705537215219], \"protect\": [[78672], 0.006297115177994306], \"during\": [[289453], 0.006246129811633153], \"will\": [[623003], 0.006138076119550942], \"health\": [[224418], 0.006137193874209476], \"help\": [[272350], 0.005655659438179383], \"your\": [[628181], 0.005291833054570317], \"&amp;\": [null, 0.00519724723359971], \"need\": [[283697], 0.005156879762602108], \"they\": [[434417], 0.004794202299168345]}, \"6\": {\"&amp;\": [null, 0.005662876242522839], \"just\": [[349792], 0.005518284506697296], \"coronavirus\": [[323447], 0.003304060228341534], \"your\": [[628181], 0.0032043179889105176], \"trump\": [[141769], 0.003146007118253158], \"from\": [[763136], 0.003116176342720684], \"positive\": [[128167], 0.002705265699191609], \"prince\": [[7850], 0.0026330267914525195], \"stay\": [[237107], 0.002358896116213406], \"stock\": [[27818], 0.0022055495413638403]}, \"7\": {\"during\": [[289453], 0.019678003723807635], \"these\": [[204803], 0.017090097976253722], \"they\": [[434417], 0.016739404562124752], \"trying\": [[93426], 0.01446676832157127], \"italy\": [[109262], 0.0142142172307333], \"share\": [[89753], 0.013882512868585386], \"people\": [[594853], 0.013221253400396692], \"friends\": [[115915], 0.01307059156281985], \"your\": [[628181], 0.01293580462243123], \"times.\": [null, 0.012754824089559831]}, \"8\": {\"cases\": [[325144], 0.01825928979187887], \"from\": [[763136], 0.012729469252257325], \"coronavirus\": [[323447], 0.00895476455179408], \"total\": [[78121], 0.006587402849292471], \"confirmed\": [[88010], 0.006249461682652039], \"positive\": [[128167], 0.005415686635763814], \"have\": [[742073], 0.005394850942401742], \"#coronaviruspandemic\": [[387131], 0.005365759566625595], \"deaths\": [[102223], 0.005043017512574802], \"first\": [[129137], 0.00419352313510375]}, \"9\": {\"#coronaviruspandemic\": [[387131], 0.012527311206543877], \"#coronavirusoutbreak\": [[350390], 0.010167756058613314], \"#coronaoutbreak\": [[56861], 0.009772170631304162], \"#coronavirusupdate\": [[104289], 0.007752654986997216], \"#coronavirusupdates\": [[42223], 0.004259623519816358], \"deaths\": [[102223], 0.003875271829108196], \"cases\": [[325144], 0.003725193282519284], \"#coronalockdown\": [[28566], 0.0036058937974563785], \"#covid\": [[62319], 0.0035772195365389], \"#corona\": [[78033], 0.003424135250031158]}, \"0\": {\"have\": [[742073], 0.01221643911012684], \"your\": [[628181], 0.00885795649364033], \"will\": [[623003], 0.008688222042160078], \"people\": [[594853], 0.008021710689365238], \"&amp;\": [null, 0.0075534472371661995], \"from\": [[763136], 0.007035480415355444], \"they\": [[434417], 0.006993945256040651], \"about\": [[459153], 0.006342564699413624], \"just\": [[349792], 0.005984527299909026], \"what\": [[367651], 0.005756468461952981]}, \"1\": {\"from\": [[763136], 0.00998083581394041], \"&amp;\": [null, 0.00881910131295745], \"will\": [[623003], 0.0075691910084945534], \"#coronavirusoutbreak\": [[350390], 0.00725309882453103], \"have\": [[742073], 0.004564686120523675], \"health\": [[224418], 0.004506690984685253], \"#coronaviruspandemic\": [[387131], 0.0042047417153742], \"help\": [[272350], 0.0040585271190841715], \"spread\": [[183906], 0.0038702471425600678], \"coronavirus\": [[323447], 0.003790780235817288]}, \"2\": {\"#coronaviruspandemic\": [[387131], 0.011159605497120091], \"your\": [[628181], 0.010588233258641897], \"about\": [[459153], 0.008389224994369586], \"from\": [[763136], 0.0057452901803035335], \"will\": [[623003], 0.005394556584011442], \"have\": [[742073], 0.004750875316316521], \"take\": [[173646], 0.004116788506239247], \"please\": [[211492], 0.004043851812768955], \"people\": [[594853], 0.003966757650897105], \"during\": [[289453], 0.0036431884913324113]}, \"3\": {\"your\": [[628181], 0.012805997968363275], \"coronavirus\": [[323447], 0.007377280107542053], \"from\": [[763136], 0.006110196165629589], \"#coronavirusoutbreak\": [[350390], 0.00602383996137531], \"help\": [[272350], 0.005671590945762276], \"stay\": [[237107], 0.005080679380373325], \"delivered\": [[22623], 0.005007891990266066], \"test\": [[99354], 0.004991877815985675], \"support\": [[160110], 0.004990203422037789], \"last\": [[102800], 0.004861281702694061]}, \"4\": {\"from\": [[763136], 0.012306571474935676], \"what\": [[367651], 0.010528956085522819], \"will\": [[623003], 0.009284862444311951], \"some\": [[206515], 0.005009477524828233], \"people\": [[594853], 0.004790936011254813], \"working\": [[107160], 0.004031479958422693], \"&amp;\": [null, 0.0035777139631744622], \"more\": [[383282], 0.003525466431066327], \"when\": [[240988], 0.0034784774015154973], \"home\": [[281001], 0.0034055024885506005]}}","topics-20": "{\"10\": {\"will\": [[623003], 0.009801330038129651], \"have\": [[742073], 0.009744982558210012], \"people\": [[594853], 0.008957141748988606], \"from\": [[763136], 0.008694174157933667], \"about\": [[459153], 0.0076270918382814286], \"&amp;\": [null, 0.006667115848060309], \"they\": [[434417], 0.006243708623168256], \"what\": [[367651], 0.006088936246390952], \"more\": [[383282], 0.00524861750281739], \"just\": [[349792], 0.0051393199759442775]}, \"11\": {\"they\": [[434417], 0.004557036973885615], \"have\": [[742073], 0.0042678196893436715], \"people\": [[594853], 0.0033600313326789767], \"social\": [null, 0.0031205994799587745], \"&amp;\": [null, 0.0027393047757744418], \"these\": [[204803], 0.001822536422807079], \"their\": [null, 0.0018114470432251011], \"distancing\": [null, 0.0017938714775778924], \"#coronavirusoutbreak\": [[350390], 0.0017225535888404843], \"what\": [[367651], 0.0017156660778340602]}, \"12\": {\"will\": [[623003], 0.004824305397064266], \"@drtedros\": [null, 0.003879087587754324], \"beat\": [null, 0.0037040384152290592], \"urgently\": [null, 0.003646576974545272], \"@who\": [null, 0.0035782936157836907], \"#coronavirusoutbreak\": [[350390], 0.003476590662417798], \"million\": [null, 0.0030557727948651225], \"pandemic\": [null, 0.0030030090949578316], \"keep\": [null, 0.00289972818679868], \"safe.\": [null, 0.0028410775532960383]}, \"13\": {\"just\": [[349792], 0.0023739849086834718], \"&amp;\": [null, 0.0018346379676840927], \"have\": [[742073], 0.0018223747495040767], \"#coronavirusoutbreak\": [[350390], 0.001575259111217501], \"from\": [[763136], 0.001506112721077222], \"will\": [[623003], 0.0014224219980916377], \"more\": [[383282], 0.001412054238597686], \"people\": [[594853], 0.0013803068431135024], \"going\": [null, 0.0013082963855443688], \"stimulate\": [null, 0.00129399951961889]}, \"14\": {\"your\": [[628181], 0.015382168853796053], \"what\": [[367651], 0.00869022119325092], \"people\": [[594853], 0.008361396432479067], \"#coronaviruspandemic\": [[387131], 0.0067873623804657005], \"doing\": [null, 0.00508846117350521], \"just\": [[349792], 0.004991271200818886], \"time\": [null, 0.0049610792810604], \"#stayathome\": [null, 0.004879345307598907], \"have\": [[742073], 0.004191830145524234], \"more\": [[383282], 0.0040525722066976095]}, \"15\": {\"from\": [[763136], 0.002976987005459862], \"they\": [[434417], 0.0028154887153478613], \"coronavirus\": [[323447], 0.002333096214209035], \"will\": [[623003], 0.0023330262931433882], \"about\": [[459153], 0.0023225477809768976], \"your\": [[628181], 0.0022663828799181483], \"have\": [[742073], 0.0022528597040431596], \"&amp;\": [null, 0.0021001345182520754], \"#coronaviruspandemic\": [[387131], 0.001672857981263011], \"#coronavirusoutbreak\": [[350390], 0.0013953951426967036]}, \"16\": {\"their\": [null, 0.0038242663624324428], \"they\": [[434417], 0.0021233495065419716], \"about\": [[459153], 0.0017967915242434063], \"people\": [[594853], 0.0017574684158546804], \"&amp;\": [null, 0.0016540919043463865], \"from\": [[763136], 0.0011473250670174173], \"during\": [[289453], 0.0011185572404853671], \"crisis\": [null, 0.0010772954065798315], \"will\": [[623003], 0.0010626319987753106], \"have\": [[742073], 0.0010554417024700624]}, \"17\": {\"from\": [[763136], 0.0032307098490551044], \"&amp;\": [null, 0.002481954342019768], \"have\": [[742073], 0.0022814887409197686], \"your\": [[628181], 0.0018415548171032364], \"will\": [[623003], 0.0017210910759619092], \"#cybersecurity\": [null, 0.0014865021809693647], \"just\": [[349792], 0.001309599588348752], \"case\": [null, 0.001282124439201299], \"more\": [[383282], 0.0011289668677362404], \"coronavirus\": [[323447], 0.0010959916793449318]}, \"18\": {\"#coronaviruspandemic\": [[387131], 0.0067104516707662395], \"listen\": [null, 0.004891869978292307], \"fight\": [null, 0.0034732818634912], \"best\": [null, 0.0029182600860394787], \"&amp;\": [null, 0.0028378227429947814], \"about\": [[459153], 0.0021795517474493264], \"great\": [null, 0.00217931776342227], \"music\": [null, 0.0020007763703977486], \"they\": [[434417], 0.0017283901938383083], \"than\": [null, 0.0017084436140857998]}, \"19\": {\"your\": [[628181], 0.011042484324242634], \"have\": [[742073], 0.01042514360326515], \"during\": [[289453], 0.007364879764145096], \"from\": [[763136], 0.007010855575207506], \"their\": [null, 0.006924781312660969], \"help\": [[272350], 0.006665391342284667], \"#coronavirusoutbreak\": [[350390], 0.006167270873727227], \"they\": [[434417], 0.006158044949213106], \"&amp;\": [null, 0.005723064690029301], \"need\": [[283697], 0.004691050819235901]}, \"0\": {\"your\": [[628181], 0.01407855288772869], \"stay\": [[237107], 0.01407544423623439], \"have\": [[742073], 0.009729928588599715], \"&amp;\": [null, 0.007462814299061865], \"home\": [[281001], 0.006274016109499833], \"safe\": [null, 0.005569565175132152], \"from\": [[763136], 0.005336343784972821], \"#coronavirusoutbreak\": [[350390], 0.005120651516192792], \"just\": [[349792], 0.0050223896820625145], \"#coronaviruspandemic\": [[387131], 0.0049411053947515365]}, \"1\": {\"from\": [[763136], 0.010275452585374769], \"&amp;\": [null, 0.009627394652359527], \"#coronavirusoutbreak\": [[350390], 0.0075903933210709415], \"will\": [[623003], 0.005923638551102204], \"health\": [[224418], 0.004502992977738049], \"#coronaviruspandemic\": [[387131], 0.004495165474588157], \"coronavirus\": [[323447], 0.004399904006418954], \"fight\": [null, 0.004190670258322003], \"spread\": [[183906], 0.0037898873033107164], \"cases\": [[325144], 0.0037764332745300385]}, \"2\": {\"#coronaviruspandemic\": [[387131], 0.015870049202186223], \"your\": [[628181], 0.006541814235963113], \"about\": [[459153], 0.004526011126606514], \"from\": [[763136], 0.004327741681481616], \"have\": [[742073], 0.004208094885926517], \"will\": [[623003], 0.004022508522908817], \"corona\": [null, 0.0036575772828902893], \"#corona\": [[78033], 0.003565864505575652], \"#coronavirusupdate\": [[104289], 0.0032988077014735233], \"world\": [null, 0.0032524238100550803]}, \"3\": {\"your\": [[628181], 0.01571565587013663], \"delivered\": [[22623], 0.009181514072611661], \"i\\u2019ll\": [null, 0.008443436286421562], \"deliver\": [null, 0.008344308879675832], \"officials\": [null, 0.007991721420729184], \"last\": [[102800], 0.007957483610676089], \"copy\": [null, 0.007643570273774338], \"signing\": [null, 0.007549604771540325], \"support\": [[160110], 0.007409275317893299], \"too:\": [null, 0.006881828956147805]}, \"4\": {\"from\": [[763136], 0.006038995133126745], \"will\": [[623003], 0.004040535158713769], \"what\": [[367651], 0.0032639687533457143], \"people\": [[594853], 0.0029360798434551988], \"&amp;\": [null, 0.002329362674701355], \"latest\": [null, 0.0018553505084481317], \"some\": [[206515], 0.001698745209421945], \"have\": [[742073], 0.0016764843371758905], \"death\": [null, 0.0016206381554931203], \"when\": [[240988], 0.0015968509568697047]}, \"5\": {\"from\": [[763136], 0.0045407003342462365], \"will\": [[623003], 0.004229868207337268], \"help\": [[272350], 0.003668505497296962], \"protect\": [[78672], 0.0026745767977358827], \"need\": [[283697], 0.0025909753807431624], \"world\": [null, 0.00250043391078553], \"&amp;\": [null, 0.002489650442689272], \"they\": [[434417], 0.002162383957946595], \"have\": [[742073], 0.002120044968491348], \"health\": [[224418], 0.001903671857231051]}, \"6\": {\"prince\": [[7850], 0.004698190873516409], \"&amp;\": [null, 0.0042001252573272896], \"charles\": [null, 0.0035059236251893124], \"positive\": [[128167], 0.0031271978937864975], \"from\": [[763136], 0.0025917467619101984], \"just\": [[349792], 0.0025651794051573984], \"your\": [[628181], 0.0022747630085402913], \"trump\": [[141769], 0.0021251291796729344], \"tests\": [null, 0.002004218413843888], \"stay\": [[237107], 0.001862839843273551]}, \"7\": {\"these\": [[204803], 0.022306722122006913], \"times.\": [null, 0.022242932965960333], \"during\": [[289453], 0.02209589745005588], \"friends\": [[115915], 0.02179392742380818], \"stand\": [null, 0.021518234861687847], \"share\": [[89753], 0.021103007965924216], \"italian\": [null, 0.020852759455981477], \"trying\": [[93426], 0.020549810920522556], \"italy\": [[109262], 0.01991017840828391], \"they\": [[434417], 0.01917414482123325]}, \"8\": {\"cases\": [[325144], 0.01848646800368714], \"from\": [[763136], 0.012638313177279674], \"coronavirus\": [[323447], 0.00979777110044529], \"#coronaviruspandemic\": [[387131], 0.00851546152483432], \"positive\": [[128167], 0.007678493108138796], \"total\": [[78121], 0.0068909571765629835], \"confirmed\": [[88010], 0.006765561402635142], \"have\": [[742073], 0.005438376015515529], \"deaths\": [[102223], 0.005169923044399549], \"first\": [[129137], 0.004676211159000489]}, \"9\": {\"myths\": [null, 0.003261079613245495], \"deaths\": [[102223], 0.002701006084050981], \"about\": [[459153], 0.0026911473752349127], \"#coronaviruspandemic\": [[387131], 0.0023443662091823304], \"have\": [[742073], 0.002172472230640127], \"cases\": [[325144], 0.002048606601009136], \"#coronaoutbreak\": [[56861], 0.001525179723297512], \"facts\": [null, 0.0015033275001879731], \"&amp;\": [null, 0.0012745973220018492], \"#coronavirusoutbreak\": [[350390], 0.0012236811220894333]}}"}'
    
    dynamodb = boto3.resource('dynamodb', region_name='us-west-1')
    table = dynamodb.Table('CSCE-678-Spark')
    response = table.query(KeyConditionExpression=Key('key').eq('history-topic-modelling-frequency-weight-sw'))["Items"][0]
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
    p3 = figure(x_range = (0, max(data3["frequencies"])), y_range = data3["words"], plot_height=600, plot_width=900)
    color_mapper = LinearColorMapper(palette = Viridis256, low = min(data3["weights"]), high = max(data3["weights"]))
    color_bar = ColorBar(color_mapper = color_mapper, location = (0, 0),ticker = BasicTicker())
    p3.add_layout(color_bar, 'right')
    p3.scatter(x = 'frequencies', y = 'words', size = 'scaled', fill_color = transform('weights', color_mapper), source = source)
    p3.add_tools(HoverTool(tooltips = [('Topic', '@topic'), ('Word', '@words'), ('Frequency', '@frequencies')]))
    p3.below[0].formatter.use_scientific = False
    script3, div3 = components(p3)
    

    return render_template('topic.html',sent_script=script3,sent_div=div3)
