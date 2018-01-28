from sqlalchemy import create_engine
from websocket import create_connection
import sqlite3
import matplotlib.pyplot as plt
from datetime import datetime
import numpy as np
from wordcloud import WordCloud, STOPWORDS
from time import gmtime, strftime, sleep
import json
import pandas as pd

# Identify the index of nearest value in an array
def find_nearest(array, value):
    idx = (np.abs(array-value)).argmin()
    return idx


# Options for running the code
collect_data = False
vis_1 = True
vis_2 = False
vis_3 = False

if collect_data == True:
    print 'Collecting Wiki data'

    #  Setup sql db wikisocket.db in current directory
    disk_engine = create_engine('sqlite:///wikisocket.db')

    #  Create table to hold information we will use
    conn = sqlite3.connect('wikisocket.db')
    c = conn.cursor()
    #  Create the data table if it doesnt exist
    c.execute('''
                CREATE TABLE  IF NOT EXISTS data(action STRING, is_anon BOOL, is_bot BOOL, page_title STRING, latitude FLOAT,
                longitude FLOAT, country_name STRING, url STRING, user STRING, time INT)
                ''')

    #  Connect to wikimon
    ws = create_connection("ws://wikimon.hatnote.com:9000")

    #  Collect data
    count = 0
    current_time = gmtime()
    sleep(5)
    while gmtime() != current_time:
        # Print count every 50 to monitor progress
        if count % 50 == 0:
            print count

        # Setup default data dictionary structure
        data = {'action': '', 'is_anon': '', 'is_bot': '', 'page_title': '', 'latitude': 'NULL', 'longitude': 'NULL',
                'country_name': 'NULL', 'url': '', 'user': '',
                'time': '%s' % (strftime("%Y-%m-%d %H:%M:%S", gmtime()))}

        #  Read in the websocket info
        data_json = (json.loads(ws.recv()))

        #  Get the relevant websocket data into the data dictionary
        for key, value in data_json.iteritems():
            # Catch the nested geo_ip dictionary
            if isinstance(value, dict):
                for key2, value2 in value.iteritems():
                    if key2 in data:
                        data[key2] = value2
            else:
                if key in data:
                    data[key] = value

        #  Write the dictionary to the sql table
        columns = ', '.join(data.keys())
        placeholders = ', '.join('?' * len(data))
        sql = 'INSERT INTO data ({}) VALUES ({})'.format(columns, placeholders)
        c.execute(sql, data.values())

        count += 1

    #  Close websocket and commit changes to database
    ws.close()
    conn.commit()
    c.close()
    conn.close()
########################################################################################################################
#  Visualisation 1: Which keywords are most common. Word cloud.
if vis_1 == True:
    print 'Running visualisation 1'

    #  Connect to db and grab all the page titles. Filter out Special:Log entries
    conn = sqlite3.connect('wikisocket.db')
    c = conn.cursor()
    c.execute('''
                SELECT page_title FROM data
                WHERE page_title NOT LIKE '%Special:Log%'
                ''')

    #  read in the titles sort out unicode
    titles =  np.array(c.fetchall(), dtype=np.unicode)
    titles = np.asarray([x[0].encode('utf-8') for x in titles])

    #  Parse the titles into a single string
    titles.astype(str).reshape(len(titles))
    titles_str = ' '.join([''.join(row) for row in titles])

    #  Define a set of words that won't be included in the word cloud
    stopwords = set(STOPWORDS)
    stopwords_list = ['User', 'sandbox', 'User talk', 'talk', 'Template', 'List', 'Talk', 'Draft', 'Wikipedia',
                      'TV series', 'History', 'video game', 'User talk', 'TV', 'series', 'video', 'game', 'u201318',
                      'u2013I', 'Articles', 'deletion', 'Editorial Team', 'Version', 'Editorial', 'Category', 'film',
                      'Tables', 'quality', 'log', 'bot', 'WP bot', 'WP', 'Project', 'wikiProject', 'File', 'Article', 'alerts']
    [stopwords.add(x) for x in stopwords_list]

    #  Create a simple word cloud (https://github.com/amueller/word_cloud)
    wc = WordCloud(background_color="white", max_words=100, stopwords=stopwords, max_font_size=50,width=800, height=400)
    wc.generate(titles_str)

    #  Make a word cloud image and save
    plt.figure(figsize=(20, 10))
    plt.imshow(wc, interpolation='bilinear')
    plt.axis("off")
    plt.savefig('./Report/Images/wordcloud.png')
    plt.clf()
    print './Report/Images/wordcloud.png'

    c.close()
    conn.close()

########################################################################################################################
#  Visualisation 2: When are most of the bad edits taking place
if vis_2 == True:
    print 'Running visualisation 2'

    #  Connect to db and grab the time and Special:Log
    conn = sqlite3.connect('wikisocket.db')
    c = conn.cursor()
    c.execute('''
                SELECT time FROM data
                WHERE page_title LIKE '%Special:Log/abusefilter%'
                ''')
    times = np.array(c.fetchall(), dtype=np.unicode)
    times_decimal = []

    #  Bin the time data into 30 minute intervals
    time_bins = np.arange(0, 24, 0.5)
    time_counts = np.zeros(len(time_bins))

    #  There is probably a faster way of doing this
    for item in times:
        time_split = (item[0]).encode().split(' ')[1].split(':')
        time_decimal = (float(time_split[0]) + float(time_split[1])/60.0)
        time_counts[find_nearest(time_bins, time_decimal)] += 1

    #  Grab data from known locations
    c.execute('SELECT time FROM data WHERE country_name != "NULL"')
    known_locations_time = np.asarray(c.fetchall())

    #  Grab data from unknown locations
    c.execute('SELECT time FROM data WHERE latitude IS "NULL"')
    unknown_locations_time = np.asarray(c.fetchall())

    # Find fraction of unknown locations for a given time
    known_time_counts = np.zeros(len(time_bins))
    for item in known_locations_time:
        time_split = (item[0]).encode().split(' ')[1].split(':')
        time_decimal = (float(time_split[0]) + float(time_split[1])/60.0)
        known_time_counts[find_nearest(time_bins, time_decimal)] += 1


    unknown_time_counts = np.zeros(len(time_bins))
    for item in unknown_locations_time:
        time_split = (item[0]).encode().split(' ')[1].split(':')
        time_decimal = (float(time_split[0]) + float(time_split[1])/60.0)
        unknown_time_counts[find_nearest(time_bins, time_decimal)] += 1

    location_fraction = np.divide(unknown_time_counts, np.add(unknown_time_counts, known_time_counts))

    #  Do some basic stats. Rolling mean and std
    #  https://www.analyticsvidhya.com/blog/2016/02/time-series-forecasting-codes-python/
    df_counts = pd.DataFrame(list(zip(time_bins, time_counts)), columns=['time', 'counts'])
    rolmean_counts = df_counts.rolling(window=10, min_periods=1).mean()
    rolstd_counts = df_counts.rolling(min_periods=2, window=10, center=False).std()

    df_location = pd.DataFrame(list(zip(time_bins, location_fraction)), columns=['time', 'fraction'])
    rolmean_location = df_location.rolling(window=10, min_periods=1).mean()
    rolstd_location = df_location.rolling(min_periods=2, window=10, center=False).std()

    # Plot time vis number of abuse logs and fraction of unknown locations
    f, ax = plt.subplots(2, sharex=True)
    ax[0].plot(time_bins, time_counts, color='black')
    ax[0].plot(time_bins, np.zeros(len(time_bins)) + np.mean(time_counts), '--', color='blue', label='Mean')
    ax[0].plot(rolmean_counts.time, rolmean_counts.counts, color='blue', label='Rolling Mean')
    ax[0].plot(time_bins, rolstd_counts.counts, color='red', label='Rolling Std')
    ax[0].legend(loc='best', prop={'size': 6})

    ax[0].set_title('Wikipedia Abuse vs. Time')
    ax[1].plot(time_bins, location_fraction, color='black')
    ax[1].plot(time_bins, np.zeros(len(time_bins)) + np.mean(location_fraction), '--', color='blue')
    ax[1].plot(rolmean_location.time, rolmean_location.fraction, color='blue', label='Rolling Mean')
    ax[1].plot(time_bins, rolstd_location.fraction+0.75, color='red', label='Rolling Std')

    ax[1].set_xlabel('Time Bins (hrs)')
    ax[0].set_ylabel('Abuse Counts')
    ax[1].set_ylabel('Fraction of Unknown Locations')
    f.savefig('./Report/Images/abuse.png')

    f.clf()
    print './Report/Images/abuse.png'
    c.close()
    conn.close()

########################################################################################################################
#  Visualisation 3: a bonus bit of code I was interested in developing. It records locations of edits on a world map
if vis_3 == True:

    #  Import these here because they are not required for vis 1 or 2
    from mpl_toolkits.basemap import Basemap
    from mpl_toolkits.axes_grid1 import make_axes_locatable

    print 'Running visualisation 3'

    #  Connect to the database
    conn = sqlite3.connect('wikisocket.db')
    c = conn.cursor()

    #  Execute simple select commands to retrieve the lat and long values
    c.execute('SELECT latitude, longitude FROM data WHERE latitude != "NULL"')
    locations = np.asarray(c.fetchall())
    #  Reformat locations
    lat = []
    lon = []
    [lon.append(x[0]) for x in locations]
    [lat.append(x[1]) for x in locations]

    #  Bin up the lat and long into 1deg regions
    #  Create empty array of coordinates
    gridx = np.linspace(-180, 180, 360)
    gridy = np.linspace(-90, 90, 180)
    # Populate the grid using the lat and lons
    grid, _, _ = np.histogram2d(lat, lon, bins=[gridx, gridy])

    # We now know how many counts there are per bin on the grid. We can now plot the grid locations with number of counts
    lat_bin = gridx[np.where(grid > 0)[0]]
    lon_bin = gridy[np.where(grid > 0)[1]]
    count_bin = grid[np.where(grid > 0)]

    # Make a plot of the world
    plt.figure(figsize=(30, 15))
    map = Basemap()
    map.drawcoastlines()
    map.drawparallels(np.arange(-90, 90, 30),labels=[1, 0, 0, 0])
    map.drawmeridians(np.arange(map.lonmin, map.lonmax+30, 60),labels=[0, 0, 0, 1])
    map.fillcontinents('white', lake_color='white')
    map.drawcountries(linewidth=1, linestyle='solid', color='black', zorder=30)
    date = datetime.utcnow()
    CS = map.nightshade(date.replace(hour=8, minute=0, second=0, microsecond=0))
    plt.title('Wikipedia Edit Locations')
    # Overlay the lat and long bins
    # Scale the size and color so they are sensible
    size = count_bin
    plt.scatter(lat_bin, lon_bin, s=size, alpha=0.8, c=count_bin, cmap='inferno', zorder=10)
    plt.colorbar(fraction=0.023, pad=0.04)
    plt.savefig('./Report/Images/editlocations.png')
    print './Report/Images/editlocations.png'
    plt.clf()
    plt.close()
    c.close()
    conn.close()
