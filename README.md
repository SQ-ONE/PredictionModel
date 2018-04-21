# PredictionModel
How to run it on PIO.
```scala
1. cd SampleProject/
2. pio build
3. pio train -- --conf spark.sql.shuffle.partitions=1000 --conf spark.buffer.pageSize=200
4. pio deploy
```
