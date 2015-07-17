One of the biggest challenges of the real world display advertising business is quantifying its impact. Unlike online advertising, with direct measurements like impressions and click thru rates, there are no easy direct measurement techniques for real world advertising.

In my team at Microsoft, these are the types of problems that we love to solve and we partnered with one of the largest physical display advertisers in Europe to work on solutions to this problem. In this post I'll describe how we combined a number of their datasets in conjunction with our preview HDInsight Spark data platform into insights around the connectedness between their ad frames that they've never had before.

First, a little background on HDInsight Spark. Spark is a cluster compute framework that is frequently named as the successor to the venerable Apache Hadoop project. In contrast to Hadoop, which does most of its data processing operations on-disk, Spark utilizes main memory as much as possible for processing and caching datasets, making it up to 100x faster for some workloads.

Spark also extends the map-reduce paradigm that Hadoop popularized with higher level primitives like sorting, joins, and set operations. This enables it to cover a wider range of workflows that previously had to be implemented as specialized systems built on top of Hadoop.

Microsoft Azure's HDInsight team also has recently released a preview of a hosted version of Apache Spark that takes all of the pain out of operating a large Spark cluster. Our team at Microsoft loves to take on projects with new platforms like this and this project was a great way for us to provide valuable feedback to our product teams.

Our advertising partner has two datasets that we used for this analysis: "frame" locations (where physical display ads exist) and location traces of a very large set of anonymous people, both within the city of Hamburg.

Spark is great for prototyping solutions like this because it is fast enough to enable interactive analysis via its command line REPL. One of the other great things about Spark is that it is really easy to get started with on your local development machine:

    $ wget http://www.apache.org/dyn/closer.cgi/spark/spark-1.4.0/spark-1.4.0-bin-hadoop2.6.tgz
    $ tar xvf spark-1.4.0-bin-hadoop2.6.tgz
    $ cd spark-1.4.0.bin-hadoop2.6
    $ bin/pyspark
    ...
    Using Python version 2.7.9rc1 (v2.7.9rc1:40eada278702, Nov 25 2014 17:25:50)
    SparkContext available as sc, HiveContext available as sqlContext.
    >>>

As you can see, I'm going to use Python for this walkthrough, but Scala and Java are other development options. Let's walk through processing this data in the REPL. The first thing we want to do is tell Spark how to load our data:

    >>> locationLines = sc.textFile('locationTraces.csv')
    >>> locationParts = locationLines.map(lambda l: l.split(","))

    >>> locations = locationParts.map(lambda l: {
        'user_id': l[0],
        'latitude': float(l[1]),
        'longitude': float(l[2]),
        'timestamp': float(l[3])
    })

    >>> frameLines = sc.textFile('frames.csv')
    >>> frameParts = frameLines.map(lambda l: l.split(","))

    >>> frames = frameParts.map(lambda l: {
        'latitude': float(l[0]),
        'longitude': float(l[1]),
        'ad_id': l[2]
    })

The first thing you'll note is that these statements return immediately. This is because Spark employs a lazy execution model: it will only execute when you actually ask for the transformed data. Until then, it just queues up the transforms you are making on the data. This enables it to potentially chain together multiple operations as it processes the data and reduces the total number of times the data needs to be transformed.

These previous statements parse the two datasets out of CSV files and into dictionary datasets for further processing. I'm using local files here on my development machine but you can parse multi-TB files out of Azure's blob storage just as easily by specifying the container (eg. 'wasb://frames@timspark.blob.core.windows.net/'). Spark will automatically iterate the files in that container and load them as if they are one long logical file.

The next thing we'd like to do is map all of the frames into geographical buckets so we can more easily process them.

    >>> framesByTileId = frames.map(frame_tile_id_mapper)

The abstraction I'm using here is a map tile mapper that follows the exact algorithm that all of the major mapping platforms use (Open Stream Maps,  Google Maps, etc.) use to divide up their maps into rows and columns at a specific zoom level. We use that concept here to map latitude, longitude pairs to a single tile id at a particular level of detail (in this case, map tiles of roughly 20m on a side).

This mapper looks like this:

    def frame_tile_id_mapper(frame):
         tileId = Tile.tile_id_from_lat_long(frame['latitude'], frame['longitude'], 19)
         return (tileId, frame['mid'] + '-' + frame['pid'])

When you return a tuple like this with two element, you can use a whole set of functions in Spark that interpret these as a key/value pair.

    >>> framesGroupedByTileId = framesByTileId.groupByKey()

We use groupByKey here to leave us with frames grouped into tile keys.  We then use this same tile mapping with the location data to build tile pairs that capture causality and link between someone visiting tile T1 and then T2.

    >>> stage1FramePairs = tilePairs.join(framesGroupedByTileId)
    >>> stage1FramePairsRemapped = stage1FramePairs.map(lambda l: (l[1][0], l[1][1]))
    >>> stage2FramePairs = stage1FramePairsRemapped.join(framesGroupedByTileId)
    >>> frameCorrelations = stage2FramePairs.map(lambda l: (l[1][0], l[1][1]))

This complex looking set of transformations essentially join in the frame location data keyed on tileId in order to come up with correlations between seeing one frame and then seeing another. We can use this to generate connection links and then sort them by frequency:

    >>> connections = frameCorrelations.flatMap(frame_correlations_to_connections_mapper)
    >>> connectionsCount = connections.reduceByKey(lambda a,b: a + b)
    >>> invertedConnectionsCounts = connectionsCount.map(lambda t: (t[1], t[0]))
    >>> sortedConnectionsCounts = invertedConnectionsCounts.sortByKey(False);

We can then have a look at the top 10 of these (you'll note if you're working along with me that this is the first command that actually causes Spark to process the data):

    >>> sortedConnectionCounts.take(10)

    '4509258-65487_to_4456583-713308': 326
    '4455992-710618_to_4498688-712933': 305
    '4455992-710618_to_4500953-745404': 305
    '4439378-63438_to_4500953-745404': 305
    '4455689-59428_to_4498688-712933': 305
    '4439378-63438_to_4498688-712933': 305
    '4455689-59428_to_4500953-745404': 305
    '4455689-59428_to_4457278-417660': 280
    '4455992-710618_to_4457278-417660': 280
    '4439378-63438_to_4457278-417660': 280

This gives us the insight that anyone seeing ad unit 4509258-65487 is highly likely to also see ad unit 4456583-713308 as they move through the city and these are insights that they do not currently have into the correlations between their advertising inventory. An advertiser like our partner can use this information to potentially build <a href="4456583-713308">Burma Shave</a> style advertising campaigns, or conversely, make sure that they spread the same ad onto the lowest correlated ad units to maximize exposure.

This is just one example of how you can combine disparate data sets like this to gain real business insights. Apache Spark makes it easy to prototype these solutions to see if they are feasible, runs these workloads faster in production, and HDInsight Spark coupled with Azure Storage makes it easy to operationalize them.

For more information, have a look at the <a href="">Apache Spark project website</a> and <a href="http://azure.microsoft.com/en-us/services/hdinsight/apache-spark"> Microsoft Azure's hosted HDInsight Spark.</a>  The code for this blog post is available on GitHub at: <a href="http://github.com/timfpark/ad-analytics-spark">http://github.com/timfpark/ad-analytics-spark</a>.