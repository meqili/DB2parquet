# DB2parquet
[Here](https://www.notion.so/d3b/f958b89d2fc343a7bcfa41e4dfaa0a90?v=951ee808eb714a7fbf3239397941f648) is the notion page that documents all databases available in VWB.

# Running command
`spark-submit
--packages io.projectglow:glow-spark3_2.12:1.1.2 --conf spark.hadoop.io.compression.codecs=io.projectglow.sql.util.BGZFCodec --conf spark.sql.broadcastTimeout=1.6174165761721435 --driver-memory 7G --executor-memory 6G --executor-cores 6 /path/to/python_script.ext -O input_file
-I /path/to/input_file.ext`
