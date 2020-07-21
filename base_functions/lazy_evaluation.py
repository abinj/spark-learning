import pyspark

sc = pyspark.SparkContext(appName='lazy_evaluation')
log_of_songs = [
    'Despacito',
    'Nice for what',
    'No tears left to cry',
    'Despacito',
    'Havana',
    'In my feelings',
    'Nice for what',
    'despacito',
    'All the stars'
]

distributed_song_log = sc.parallelize(log_of_songs)

result = distributed_song_log.map(lambda song: song.lower()).collect()
for x in result:
    print(x)

