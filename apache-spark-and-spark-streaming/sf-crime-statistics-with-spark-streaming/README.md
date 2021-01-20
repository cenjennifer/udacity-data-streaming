
## How did changing values on the SparkSession property parameters affect the throughput and latency of the data?

Increasing maxRatePerPartition (max # of messages per partition per batch) and maxOffsetPerTrigger 
can speed up the data ingestion, which can increase the throughput and processedRowsPerSecond 
(rate at which Spark process the data). The impact this may provide can be dependent on the data size. 
Also, these parameters shouldn't be set to an extreme as this can slow down the batches completion 
(especially if there's a lot of data).

## What were the 2-3 most efficient SparkSession property key/value pairs? Through testing multiple variations on values, how can you tell these were the most optimal?

Increasing `maxRatePerPartition` to 200 and `spark.default.parallelism` seems have the best results
in the summary metrics table in Spark UI. 

Increasing `spark.executor.memory` / `spark.driver.memory` on executor or driver didn't really have any impact.
Looking at the executors tab in Spark UI, the memory used isn't close to the amount of memory available.
The reason can be the small dataset size. Since memory usage was low, there isn't any need for more nodes to improve performance.
