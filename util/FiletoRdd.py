from GetContext import SparkHiveContext

obj = SparkHiveContext()

class ReadFileFormat:

    def csvToRdd(self,file):
        self.file = file
        sc = obj.getSparkContext()
        rdd= sc.textFile(file)
        print rdd.collect()


    def jsonToRdd(self,file):
        self.file = file
        sc = obj.getSparkContext()
        rdd = sc.textFile(file)
        print rdd.collect()

    def collectionToRdd(self,list=[], *args):
        self.list =list
        sc = obj.getSparkContext()
        rdd = sc.parallelize(list)
        print rdd.collect()