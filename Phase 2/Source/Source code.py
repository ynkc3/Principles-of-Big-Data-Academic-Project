from plotly.data import gapminder
import tkinter
from pyspark.shell import sqlContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, coalesce
import matplotlib
matplotlib.use('TKAgg')
import matplotlib.pyplot as plt
import pandas as pd
import plotly.express as px

spark_session = SparkSession.builder.master("local").appName("Twitter Data Analysis").config("spark.debug.maxToStringFields", "1000000000").getOrCreate()
data_frame = spark_session.read.json("/home/yagnasri/Desktop/extracted_tweets.json")
data_frame.createOrReplaceTempView("Cricket_Premier_League")

#Query1
q1 = sqlContext.sql("SELECT place.country AS country, SUM(user.followers_count) AS followers_count FROM Cricket_Premier_League WHERE place.country != 'null' GROUP BY place.country ORDER BY followers_count DESC LIMIT 10")
q1.show()
df1 = q1.toPandas()
df1.to_csv('query1_output.csv', index=False)

#Query2
q2 = sqlContext.sql(" SELECT COUNT(*) AS tweets_count, 'Indian Premier League' AS league FROM Cricket_Premier_League WHERE TEXT LIKE '%ipl%' OR TEXT LIKE '%Indian Premier League%' OR  TEXT LIKE '%IPL%' OR TEXT LIKE '%Ipl%' UNION SELECT COUNT(*) AS tweets_count, 'Pakistan Super League' AS league FROM Cricket_Premier_League WHERE TEXT LIKE '%psl%' OR TEXT LIKE '%Pakistan Super League%' OR TEXT LIKE '%PSL%' OR TEXT LIKE '%Psl%' UNION  SELECT COUNT(*) AS tweets_count, 'Caribbean Premier League' AS league FROM Cricket_Premier_League WHERE TEXT LIKE '%cpl%' OR TEXT LIKE '%Caribbean Premier League%' OR TEXT LIKE '%CPL%' OR TEXT LIKE '%Cpl%' UNION SELECT COUNT(*) AS tweets_count, 'Bangladesh Premier League' AS league FROM Cricket_Premier_League WHERE TEXT LIKE '%bpl%' OR TEXT LIKE '%Bangladesh Premier League%' OR  TEXT LIKE '%BPL%' OR TEXT LIKE '%Bpl%' UNION SELECT COUNT(*) AS tweets_count, 'Ashes' AS league FROM Cricket_Premier_League WHERE TEXT LIKE '%ashes%' OR TEXT LIKE '%Ashes%' OR  TEXT LIKE '%ASHES%' UNION SELECT COUNT(*) AS tweets_count, 'Big Bash League' AS league FROM Cricket_Premier_League WHERE TEXT LIKE '%bbl%' OR TEXT LIKE '%Big Bash League%' OR  TEXT LIKE '%BBL%' OR TEXT LIKE '%Bbl%' OR TEXT LIKE '%big bash%'")
q2.show()
df2 = q2.toPandas()
df2.to_csv('query2_output.csv', index=False)

#Query3
q3 = sqlContext.sql(" SELECT COUNT(*) AS tweets_count, 'Virat Kohli' AS batsman_name FROM Cricket_Premier_League WHERE TEXT LIKE '%virat kohli%' OR TEXT LIKE '%Virat Kohli%' OR  TEXT LIKE '%virat%' OR TEXT LIKE '%kohli%' UNION SELECT COUNT(*) AS tweets_count, 'David Warner' AS batsman_name FROM Cricket_Premier_League WHERE TEXT LIKE '%david warner%' OR TEXT LIKE '%David Warner%' OR TEXT LIKE '%warner%' OR TEXT LIKE '%WARNER%' UNION  SELECT COUNT(*) AS tweets_count, 'Kane Williamson' AS batsman_name FROM Cricket_Premier_League WHERE TEXT LIKE '%Kane Williamson%' OR TEXT LIKE '%kane williamson%' OR TEXT LIKE '%kane%' OR TEXT LIKE '%williamson%' UNION SELECT COUNT(*) AS tweets_count, 'Joe Root' AS batsman_name FROM Cricket_Premier_League WHERE TEXT LIKE '%Joe Root%' OR TEXT LIKE '%joe root%' OR  TEXT LIKE '%root%' OR TEXT LIKE '%Root%' UNION SELECT COUNT(*) AS tweets_count, 'Shikhar Dhawan' AS batsman_name FROM Cricket_Premier_League WHERE TEXT LIKE '%Shikhar Dhawan%' OR TEXT LIKE '%shikhar dhawan%' OR  TEXT LIKE '%shikhar%' OR  TEXT LIKE '%dhawan%' OR  TEXT LIKE '%gabbar%' UNION SELECT COUNT(*) AS tweets_count, 'David Miller' AS batsman_name FROM Cricket_Premier_League WHERE TEXT LIKE '%David Miller%' OR TEXT LIKE '%david miller%' OR  TEXT LIKE '%Miller%' OR TEXT LIKE '%miller%' OR TEXT LIKE '%Miller%' ORDER BY tweets_count DESC")
q3.show()
df3 = q3.toPandas()
df3.to_csv('query3_output.csv', index=False)

#Query4
q4 = sqlContext.sql("SELECT user.name AS User_name, user.verified as verfied_status FROM Cricket_Premier_League WHERE user.verified = 'true'")
q4.show()
df4 = q4.toPandas()
df4.to_csv('query4_output.csv', index=False)

#Query5
q5 = sqlContext.sql("SELECT user.name AS User_name, retweeted_status.retweet_count as retweets_count FROM Cricket_Premier_League ORDER BY retweets_count DESC")
q5.show()
df5 = q5.toPandas()
df5.to_csv('query5_output.csv', index=False)

#Query6
q6 = sqlContext.sql("SELECT retweeted_status.source AS retweet_source, count(*) AS count FROM Cricket_Premier_League where retweeted_status.source IS NOT null GROUP BY retweeted_status.source ORDER BY count DESC")
q6.show()
df6 = q6.toPandas()
df6.to_csv('query6_output.csv', index=False)

#Query7
q7 = sqlContext.sql("SELECT user.id, count(*) AS count FROM Cricket_Premier_League where user.id IS NOT null GROUP BY user.id ORDER BY count DESC")
q7.show()
df7 = q7.toPandas()
df7.to_csv('query7_output.csv', index=False)

#Query8
q8 = sqlContext.sql("SELECT user.geo_enabled, count(*) from Cricket_Premier_League group by user.geo_enabled")
q8.show()
df8 = q8.toPandas()
df8.to_csv('query8_output.csv', index=False)

#Query9
q9 = sqlContext.sql("SELECT possibly_sensitive, count(*) AS count FROM Cricket_Premier_League GROUP BY possibly_sensitive")
q9.show()
df9 = q9.toPandas()
df9.to_csv('query9_output.csv', index=False)

#Query10
q10 = sqlContext.sql("select substring(user.created_at,0,4) as Name_of_Week_Day, count(*) as Count from Cricket_Premier_League group by substring(user.created_at,0,4) order by Count desc")
q10.show()
df10 = q10.toPandas()
df10.to_csv('query10_output.csv', index=False)