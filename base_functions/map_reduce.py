from pyspark import SparkConf, SparkContext

if __name__ == "__main__":
    conf = SparkConf().setAppName("map-reduce").setMaster("local[2]")
    sc = SparkContext(conf=conf)
    