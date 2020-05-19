from pyspark import SparkConf, SparkContext

if __name__ == "__main__":
    conf = SparkConf().setAppName("Word Count").setMaster("local[2]")
    sc = SparkContext(conf=conf)
    lines = sc.textFile("dataset/word_count.text")
    words = lines.flatMap(lambda line: line.split(" "))
    word_counts = words.countByValue()

    for word, count in word_counts.items():
        print("{} : {}".format(word, count))
