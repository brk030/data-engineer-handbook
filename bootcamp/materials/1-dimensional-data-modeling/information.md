Day 1 - Lecture
https://www.youtube.com/watch?v=5U-BbZ9G_xU&list=PLwUdL9DpGWU0lhwp3WCxRsb1385KFTLYE&index=3&ab_channel=DatawithZach
Notes: 
  - Talking about Dimensional Data Modelling not Fact Modelling 
What is a Dimension?
- dimensions are attributes of an entity
- dimensions come in two flavors - either slowly-changing or fixed
  - slowly-changing attributed are time dependent
  - e.g. for fixed is the birthday, manufacturer of a phone

Modelling of data should depend for whom you will be serving
- Data Analyst/ Data Scientist - should be very easy to query; not many complex data types
- Data Engineers - should be compact and harder to query (nested types are okay)
- ML Models - Depends on the model and how it is trained
- Customers - depends on the customer but not analytical a chart should be preferred

OLTP vs OLAP vs Master Data
- OLTP (Online Transactional Processing) - optimizes for low-latency (response with minimal delay); low-volume queries
  - more used in software engineering where it is optimized for a user
- OLAP (Online Analytical Processing) - optimizes for large volume prefers group by and minimizes join's
  - more for analysis which look at a big chunk of data 
- Master Data 
  - optimizes for completeness of entity definitions; deduped
  - serves as a middle ground between OLTP and OLAP, providing a complete and normalized view of data for analytical 
  purposes

OLTP and OLAP is a continuum
Production Database Snapshots -> Master Data -> OLAP Cubes (slice and dice) -> Metrics 

Cumulative Table Design
- maintain a complete history of dimensions, allowing for the tracking of changes over time
- created by performing a full outer join between today's and yesterday's data tables 
  - yesterday can be all cumulative days until yesterday or null 
- can have filtering criterias so that the table does not get unendlessly bigger

Run Length Encoding
- This technique compresses data by storing the value and the count of consecutive duplicates, which is particularly 
useful for temporal data
- Shuffling (Spark Shuffle etc.) will break the benefits of the parquet ordering 
  - one can either resort the data or the data is in an array which gets exploded

Day 2 - Lecture 
https://www.youtube.com/watch?v=emQM9gYh0Io&t=352s&ab_channel=DatawithZach


Types of Dimensions
- Stable Dimensions - attributes that do not change, such as a person's birthday
- Changing Dimensions - attributes that evolve over time, requiring careful modeling to track changes 

Idempotent pipelines
- denoting an element of a set which is unchanged in value when multiplied or otherwise operated on by itself 
  - pipelines should always produce the same results 

Problems with idempotent pipelines 
- silent failure, it does not fail it just produces incorrect/different/non-reproducible data 

Best Practices for building idempotent pipelines
- avoid insert into without truncate 
  - using `INSERT INTO` without clearing previous data can lead to duplication and non-idempotency
- use merge (do not write duplicares into the table) and insert overwrite
  - These methods help maintain idempotency by ensuring that data is updated correctly without duplication
- implement proper date ranges
  - always include both start and end dates in your queries to avoid unbounded data retrieval, which can lead to inconsistencies
- check for complete input sets
  - ensure all necessary input data is available before running the pipeline to avoid incomplete data processing
- be careful about dependencies on past data
  - cumulative data depends on previous days and cannot run parellel, which might make it inefficient running 

Consequences of non-idempotent pipelines
- backfilling causes inconsistencies between the old and restated data and it's very hard to troubleshoot bugs. 18:33 - 19:10
- unit testing cannot replicate the production behavior. 19:14 - 19:44
- silent failures

Modeling slowly changing dimensions 
- Types of SCDs:
  - Type 0 - fixed dimensions that do not change (e.g., a person's birth date)
  - Type 1 - only the latest value is stored (overwrite), which can lead to loss of historical data -> leads to not idempotent pipelinea 
  - Type 2 - maintains historical data with start and end dates (such as. 9999-12-31) for each change, allowing for accurate backfilling and idempotency
  - Type 3 - stores current and original values but loses historical context if changes occur more than once, making it non-idempotent


Day 3

What makes dimensions additive?
- dimension is additive over a specific window of time, if and only if, the grain of data over that window can only ever
be one value at a time
- additive dimensions mean that you do not "double count"
  - example additive:
    - population is equal to 20 year olds + 30 year olds + ...
  - example non-additive:
    - number of active users != number of users on web + number of users on android + ...

How does additivity help?
- do not need to use COUNT(DISTINCT) on preaggregated dimensions
- non-additive dimensions are usually only non-additive with respect to COUNT-aggregations but not to SUM-aggregations

When & why should you use enums?
- enums are great for low-to-medium cardinality (up to 50 is a good number)
- build in data quality (if you write a value that does not fit into enum the operation fails)
- build in static fields 
- build in documentation (you have an exhaustive list and cannot miss a value)

ToDo - read a pipeline schema of Zac: https://github.com/EcZachly/little-book-of-pipelines/tree/master
ToDo - read about flexible Schema

How is graph data modeling different?
- graph modeling is relationship focused and not entity focused (do not care about columns)
- because of this you can do a very poor job at modeling the entities, usually the model looks like this:
  - identifier: String
  - type: String
  - properties: MAP <String, String>
- relationships are modeled a bit more in depth:
  - subject_identifier: String
  - subject_type: Vertex_Type
  - object_identifier: String
  - object_type: vertex_type
  - edge_type: edge_type
  - properties: MAP <String, String> 