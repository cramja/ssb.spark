# Run the SSB Benchmark on Spark.

To use:

```shell
sbt package
SSB=`pwd`/target/scala-2.10/ssb-queries_2.10-1.0.jar

# Go to your spark directory.
./bin/spark-submit --class "SSB" --master local[4] $SSB
```
