# MyDBSCAN
dbscan algorithm on Spark in Scala
本算法在这个[https://github.com/irvingc/dbscan-on-spark](https://github.com/irvingc/dbscan-on-spark)的基础之上修改，
原来的算法从原始数据集中能够读取和保存的数据只有两个，两个纬度的数据。本人在相应的类上面增加了两个一个字段，用于保存这两个纬度所对应的其他信息，比如时间，id，等等。
