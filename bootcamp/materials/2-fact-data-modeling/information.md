Fact Data Modelling

What is a fact?
- think of a fact as something that happened or occured
- are not slowly changing which makes them easier to model than dimensions in some respect
  - the past cannot be changed

What makes fact modelling hard?
- fact data is usually 10-100x (general rule) the volume of dimension data
- fact data can need a lot of context for effective analysis
- duplicates in facts are way more common than in dimensional data 
  - most difficult step of working with fact data is the decoupling step

How does fact data modelling work?
- there are two types of modelling, normalization and denormalization
- normalized facts - do not have any dimensional attributes, just IDs to join to get that information
- denormalized facts - bring in some dimensional attributes for quicker analysis at the cost of more storage
- raw logs and fact data are not the same
  - raw logs: ugly schemas designed for online systems - can be stuff logged by software engineers
  - fact data: nice column names and should be smaller than raw logs; parse out hard-to-understand 
  columns such as json string
- can be thought of as "who", "where", "how", "what", "when"
  - "who" fields are usually pushed out as an IDs
  - "where" fields are modelled similar to "who" IDs to join and bring in more dimensions
  - "how" fields are similar to "where" fields (e.g. he used an iphone to make this click)
  - "what" fields are fundamentally part of the nature of the fact, what happened (e.g. "send", "clicked", ...)
  - "when" fields are fundamentally part of the nature of the fact, are mostly modelled as timestamps (should
  be always logged as utc)
  - "what" & "when" fields should not be "null"

When should you model in dimensions?
- brings in all the critical context for your fact data
- do not log everything
- logging should conform to values specified by the online teams -> "Thrift" might be a solution cross teams
  - Thrift allows you to define data types and service interfaces for different languages

Potential options when working with high volumen data
- sampling
  - does not work for all use-cases such as in security (needle in the hay sack problem)
  - works best for metric-driven use-cases where impreision is not an issue
  - works due to the law of large numbers
- bucketing
  - fact data can be bucketed by one of the important dimensions (usually the "who fields")
  - bucket joins can be much fester than shuffle joins
  - sorted-merge bucket joins can do joins without Shuffle at all
- How long should you hold onto fact data?
  - high volume make fact data much more costly to hold onto for a long time
  - approach of big tech:
    - any fact table < 10 TB was hold onto for 60-90 days
    - any fact table > 100 TV was hold onto for less than 14 days

Deduplication of fact data
- facts can often be duplicated (e.g. you can click a notification multiple times)
- how do you pick the right window for deduplication?
  - looking at distributions of duplicates is a good idea (no duplicated in a day? an hour? a week?)
- intrday deduping options: microbatch (hourly) & streaming (lower bases than microbatch)

Streaming to deduplicate facts
- streaming allows you to capture most duplicates in a very efficient manner
  - windowing matters here (15 minute to hourly are usually sweet spots)
  - entire day duplicates can be harder for streaming because it needs to hold onto such a big window of memory
  - large memory of duplicates usually happen within a short time of first event

Properties of Facts vs Dimensions
- Dimensions
  - usually show up in "GROUP BY" when doing analytics
  - can be "high cardinality" or "low cardinality" depending
  - generally come from a snapshot state

- Facts
  - usually aggregated when doing analytics by things like "SUM", "AVG", "COUNT"
  - almost always higher volume than dimensions
  - generally come frpm events and logs

Date List data structure
- extremely efficient way to manage user growth
- e.g. could be seeing when people have been online the last time which could look as follows:
  - user_id, date, datelist_int -> 32, 2023-01-01, 100000010000010 
  - In the above every 1 represents the activity starting from Janauary, 1st and going back into the year 2022
    - By doing it this way, you can save 30 days of data as an integer type -> helps with compression 

Why should shuffle be minimized?
- big data leverages parallesism as much as it can and shuffling can be a bottle neck 

What types of queries are highly parallelizable
- extremly parallel: SELECT, FROM, WHERE
- kinda parallel: GROUP BY, JOIN, HAVING
- painfully not parallel: ORDER BY -> if possible should not be used

How do you make GROUP BY more efficient?
- give GROUP BY some buckets and guarantees -> can be done in S3, IceBerg, Spark and all
  - bucketing happens on a high cardinality feature field
  - modulus grouping happens when writing the data out, so we do not have to shuffle during group by because it is
  already set in the correct bucket

How to use reduced fact data modeling
- fact data often has this schema 
  - user_id, event_time, action, date_partition
  - very high volume, 1 row per event
- daily aggregate often has this schema
  - user_id, action_count, date_partition
  - medium sized volume, 1 row per user per day
- reduced fact take this one step further
  - user_id, array action_count for every day in time horizon, month_start_partition/ year_start_partition
  - low volume, 1 row oer user per month/ year
  - this type of analysis unlocks root cause analysis