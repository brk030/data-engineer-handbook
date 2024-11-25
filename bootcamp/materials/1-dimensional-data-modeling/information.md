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