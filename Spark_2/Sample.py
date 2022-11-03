from pyspark import SparkContext, SparkConf



def context():
    conf = SparkConf().setAppName("Spark_2").setMaster("local[*]")
    sc = SparkContext(conf=conf)
    return sc


sc = context()


# loading the file into RDD

def Parse(line):
    line = line.replace(' -- ', ',')
    line = line.replace('.rb: ', ',')
    line = line.replace(', ghtorrent-', ',')
    return line.split(',', 4)  # max split in index , here 0,1,2,3


def gh_Rdd(filename):
    textFile = sc.textFile(r"C:\Users\HARI\Downloads\assign_files\ghtorrent-logs.txt")
    parsedRdd = textFile.map(Parse)
    return parsedRdd


rowRDD = gh_Rdd(filename="ghtorrent-logs.txt").cache()  # RDD
# sample = rowRDD.take(20)
'''for word in sample:
    print("words: ", word)'''


# How many lines does the RDD contain
def count_RDDs():
    rc = rowRDD.count()
    return rc


rdd_count = count_RDDs()
print("RDDs count=", rdd_count)


# count the number of WARNing messages
def num_warn():
    numWARN = rowRDD.filter(lambda x: x[0] == 'WARN')
    return numWARN


nw = num_warn()
print("num of WARNing: ", nw.count())


# How many repositories where processed in total? Use the api_client lines only

def repos(x):
    try:
        # re-write with Split by looking for api_client
        split = x[4].split('/')[4:6]  # position in 4,5 acc to index
        join = '/'.join(split)
        res = join.split('?')[0]
    except:
        res = ''
    x.append(res)
    return x


# filter the rows
filterrdd = rowRDD.filter(lambda x: len(x) == 5)

# filter out the only api_client
api_client = filterrdd.filter(lambda x: x[3] == "api_client")

# Add another column with repo otherwise ''
reposRDD = api_client.map(repos)

# filter the rows without Repo
filter_api = reposRDD.filter(lambda x: x[5] != '')

# groupby repo and count of repo
repos_count = filter_api.groupBy(lambda x: x[5])
print("count of api_client repos: ", repos_count.count())

# which client did most HTTP requests?

http_request = api_client.groupBy(lambda x: x[2])


http_sum = http_request.map(lambda x: (x[0], len(x[1])))

print("HTTPs most requests:", http_sum.max(key=lambda x: x[1]))

# Which client did most FAILED HTTP requests? Use group by to provide an answer

failed = api_client.filter(lambda x: x[4].split(' ', 1)[0] == 'Failed')  # filter the failed ones


# group by the failed ones
user_failed = failed.groupBy(lambda x: x[2])
user_failed_sum = user_failed.map(lambda x: (x[0], len(x[1])))

# count the failed ones
print("HTTPS failed requests : ", user_failed_sum.max(key=lambda x: x[1]))


# What is the most active hour of day

def active_hour(x, toAdd):
    x.append(toAdd)
    return x


# split the timestamp to hour only
hour = filterrdd.map(lambda x: active_hour(x, x[1].split('T', 1)[1].split(':')[0]))

# groupby the hours

group_hour = hour.groupBy(lambda x: x[5])

hours_count = group_hour.map(lambda x: (x[0], len(x[1])))
print("Active Hours : ", hours_count.max(key=lambda x: x[1]))

# What is the most active repository
active_repos = filter_api.groupBy(lambda x: x[5])

# count of Active repos
count_repos = active_repos.map(lambda x: (x[0], len(x[1])))
print("count of Active repositories: ", count_repos.max(key=lambda x: x[1]))