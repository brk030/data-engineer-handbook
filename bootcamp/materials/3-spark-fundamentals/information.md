What is Apache Spark?
- distributed compute framework that allows you to process very large amounts of data efficiently

Why is Spark so good?
- leverages RAM much more efficiently 

How does Spark work?
1. the plan (e.g. the script)
   - is the transformation you describe in Python, Scala or SQL
   - plan is evaluated lazy - execution only happens when it needs to
   - when does the execution need to happen?
     - writing output
     - part of the plan depends on data itself e.g. calling dataframe.collect()
     - 
2. the driver
    - reads the plan
    - important spark driver settings
      - spark.driver.memory: For complex jobs or jobs that use dataframe.collect(), you may need to bump this higher
      or else you will experience an OOM
      - spark.driver.memoryOverheadFactor - What fraction the driver needs for a non-heap related memory, usually 10%,
      might need to be higher for complex jobs
    - driver needs to determine a few things
      - when to actually start executing the job and stop being lazy
      - how to "JOIN" datasets
      - how much parallelism each step needs
3. the executors 
   - driver passes the plan to the executors to do the work
   - important executor settings
     - spark.executor.memory: This determines how much memory each executor gets. A low number here may cause Spark to 
     "spill to disk" which will cause your job to much slower
     - spark.executor.cores: How many tasks can happen on each machine - default is 4, should not go higher than 6
     - spark.executor.memoryOverheadFactor: What percentage of memory should an executor use for non-heap related tasks, 
     usually 10%. For jobs with lots of UDFs and complexity, you may need to bump this up!

The types of JOINs in Spark
1. Shuffle sort-merge 
   - default JOIN strategy since Spark 2.3
   - least performant, only works good when both sides of the join are large
2. Broadcast Hash Join
   - work well if the one side of the join is small (small refers 8 to 10 GB)
   - spark.sql.autoBroadcastJoinThreshold: Default ist 10 MBs, but it can go as high as 8 GBs (you will 
   experience weird memory problems > 1 GBs)
   - join **without** shuffle
3. Bucket Joins
   - join **without** shuffle

How does shuffle work? -> default case
1. You have input files, which get mapped such as creating a new columns, where-clauses etc.
2. second step is the reduce step (such as GROUP BY), the file gets divided by number and the remainder is on which
partition it ends up

Shuffle 
- shuffle partitions and parallelism are linked
- shuffle partitions and parallelism
  - spark.sql.shuffle.partitions and spark.default.parallelism
  - spark.sql.shuffle.partitions should be used since the other is related to the RDD api, which should not be 
  used/ touched (there are only tiny edge cases where RDD api should be used)
- shuffle should be used in low-to-medium volumes - makes everything a lot easier
- shuffle should not be used in high volumnes > 10 TBs

How to minimize shuffle at high volumes?
- bucket data if **multiple** JOINs or aggregations are happening downstream
- spark has the ability to bucket data to minimize or eliminate the need for shuffle when doing JOINs
- bucket joins are very efficient but have drawbacks
  - main drawback is the initial parallelism = number of buckets
  - bucket joins only work if the two tables number of buckets are multiples of each other (always use 
  powers of 2 for number of buckets)

Shuffle and Skew
- sometimes some partitions have dramatically more data than others, which can happen due:
  - not enough partitions
  - natural way the data is (e.g. Beyonce get more notifications than the average Facebook  user)

How to tell your data is skewed?
- most common is a job getting to 99%, taking forever and failing
- more scientific approach is to do a box-and-whiskers plot of the data to see if there's any extreme outliers

Ways to deal with Skew
- adaptive query execution (only in Spark 3+) with the setting "spark.sql.adaptive.enabled=True"
  - only use if needed, makes the job more expensive to run
- salting the GROUP BY - best option before Spark 3

How to look at Spark query plans
- use explain() on your dataframes - will show you the join strategies that Spark will take

Spark output datasets
- should almost always be partioned on "date"
  - date should be the execution date of the pipeline (in big tech this is called "ds partitioning")