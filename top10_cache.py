from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.types import IntegerType
import pandas as pd
import praw

## PRAW
#Get posting karma of the creator from Python Reddit API Wrapper
def udf_get_karma(author_str):
    #Reddit API Credential
    client_id = 'UViHJJ217cLodAY9AraMWg'
    client_secret = 'dvpLKVfv7ItyQhHkYPPOK6o_ssI63w'
    user_agent = 'android:com.example.myredditapp:v1.2.3 (by u/sf_suzyfeng)'
    #Reddit Username
    username = 'sf_suzyfeng'
    #Initialize a Reddit instance
    reddit = praw.Reddit(client_id=client_id,client_secret=client_secret,user_agent=user_agent)
    try:
        redditor= reddit.redditor(author_str)
        link_karma= redditor.link_karma
    #if the account was deleted or ..., set Karma to 0 
    except:
        link_karma= 0
    return link_karma

#Creter udf to retrieve Karma 
udf_karma = F.udf(udf_get_karma, IntegerType())


## Top 10 creators
# Preparation
sampled_submissions_creators= sampled_submissions.select("subreddit","created_utc","author","title", "score", "num_comments")
sampled_submissions_creators = sampled_submissions_creators.withColumn("score",F.col("score").cast("float"))\
                                         .withColumn("num_comments",F.col("num_comments").cast("float"))\
                                         .withColumn("created_year",F.year(F.to_timestamp(F.col("created_utc").cast('int'))))

#Group submissions data by year and author
sampled_submissions_creators= sampled_submissions_creators.groupBy("created_year","author").agg(
    F.count("*").alias("sub_count"),
    F.sum("score").alias("total_score"),
    F.sum("num_comments").alias("total_num_cmnt"),
    F.round(F.avg("score"),2).alias("avg_score"),
    F.round(F.avg("num_comments"),2).alias("avg_num_cmnt")
).sort('created_year')

# Filter out meaningless data
sampled_submissions_creators= sampled_submissions_creators.filter((sampled_submissions_creators['author']!='[deleted]') &\
                                                                  (sampled_submissions_creators['total_score']!=0) &\
                                                                  (sampled_submissions_creators['total_num_cmnt']!=0))

# Designed for the sample dataset: More straightforward to show the quality of creators' contents for the sample
sampled_submissions_creators= sampled_submissions_creators.withColumn("creator_score",F.col("sub_count")*(F.col("total_score")+F.col("total_num_cmnt")))\
                                                          .orderBy("creator_score",ascending= False)

# Designed for the full dataset: Considering the quantity of comments and scores varies, it's more reasonable to use ranking than simplly adding up the quantity.
# sampled_submissions_creators= sampled_submissions_creators.withColumn("sub_count_ranking",F.dense_rank().over(Window.orderBy("sub_count")))\
#                                                           .withColumn("avg_score_ranking",F.dense_rank().over(Window.orderBy("avg_score")))\
#                                                           .withColumn("avg_num_cmnt_ranking",F.dense_rank().over(Window.orderBy("avg_num_cmnt")))\
#                                                           .withColumn("creator_score",F.col("sub_count_ranking")+F.col("avg_score_ranking")+F.col("avg_num_cmnt_ranking"))\
#                                                           .withColumn('posting_karma',udf_karma(F.col("author")))\
#                                                           .orderBy("creator_score",ascending= False)\

sampled_submissions_creators.cache()

#Generate top10 creators of the year
sampled_creators_top10_2021= sampled_submissions_creators.filter(sampled_submissions_creators['created_year']==2021)\
                                                         .select('created_year',"author","sub_count","avg_score","avg_num_cmnt","creator_score")\
                                                         .limit(10)\
                                                         .withColumn('posting_karma',udf_karma(F.col("author")))\
                                                         .toPandas()
sampled_creators_top10_2022= sampled_submissions_creators.filter(sampled_submissions_creators['created_year']==2022)\
                                                         .select('created_year',"author","sub_count","avg_score","avg_num_cmnt","creator_score")\
                                                         .limit(10)\
                                                         .withColumn('posting_karma',udf_karma(F.col("author")))\
                                                         .toPandas()
sampled_creators_top10_2023= sampled_submissions_creators.filter(sampled_submissions_creators['created_year']==2023)\
                                                         .select('created_year',"author","sub_count","avg_score","avg_num_cmnt","creator_score")\
                                                         .limit(10)\
                                                         .withColumn('posting_karma',udf_karma(F.col("author")))\
                                                         .toPandas()
#Generate top10 creators of the year from 2021 to 2023
sampled_creators_top10_total= pd.concat([sampled_creators_top10_2021,sampled_creators_top10_2022,sampled_creators_top10_2023], axis=0, ignore_index=True)

#Generate top100 creators of the year, proportional to the duration
sampled_creators_top46_2021= sampled_submissions_creators.filter(sampled_submissions_creators['created_year']==2021)\
                                                         .select('created_year',"author","sub_count","avg_score","avg_num_cmnt","creator_score")\
                                                         .limit(46)\
                                                         .withColumn('posting_karma',udf_karma(F.col("author")))\
                                                         .toPandas()
sampled_creators_top46_2022= sampled_submissions_creators.filter(sampled_submissions_creators['created_year']==2022)\
                                                         .select('created_year',"author","sub_count","avg_score","avg_num_cmnt","creator_score")\
                                                         .limit(46)\
                                                         .withColumn('posting_karma',udf_karma(F.col("author")))\
                                                         .toPandas()
sampled_creators_top8_2023= sampled_creators_top10_2023[0:8]
#Generate top100 creators of the year from 2021 to 2023
sampled_creators_top100_total= pd.concat([sampled_creators_top46_2021,sampled_creators_top46_2022,sampled_creators_top8_2023], axis=0, ignore_index=True)


# List of top100 creators from 2021 to 2023, remove duplicate name
sampled_topcreators= sampled_creators_top100_total['author'].to_list()
sampled_topcreators = list(set(sampled_topcreators))

#Generate top100 creators of the year from 2021 to 2023, proportional to the duration
sampled_creators_top46_2021= sampled_submissions_creators.filter(sampled_submissions_creators['created_year']==2021)\
                                                         .select('created_year',"author","sub_count","avg_score","avg_num_cmnt","creator_score")\
                                                         .limit(46)\
                                                         .withColumn('posting_karma',udf_karma(F.col("author")))\
                                                         .toPandas()
sampled_creators_top46_2022= sampled_submissions_creators.filter(sampled_submissions_creators['created_year']==2022)\
                                                         .select('created_year',"author","sub_count","avg_score","avg_num_cmnt","creator_score")\
                                                         .limit(46)\
                                                         .withColumn('posting_karma',udf_karma(F.col("author")))\
                                                         .toPandas()
sampled_creators_top8_2023= sampled_creators_top10_2023[0:8]
sampled_creators_top100_total= pd.concat([sampled_creators_top46_2021,sampled_creators_top46_2022,sampled_creators_top8_2023], axis=0, ignore_index=True)

# List of top100 creators from 2021 to 2023, remove duplicate name
sampled_topcreators= sampled_creators_top100_total['author'].to_list()
sampled_topcreators = list(set(sampled_topcreators))




## New varaibles
# Data preparation
sampled_submissions_var= sampled_submissions.select("author","selftext","created_utc")\
                                        .withColumn("created_hour",F.hour(F.to_timestamp(F.col("created_utc").cast('int'))))\
                                        .withColumn('len',F.length(F.col('selftext')))

# Dummy submissions varaibles
sampled_submissions_var= sampled_submissions_var.withColumn("skincare",F.col("selftext").rlike("""(?i)body|(?i)hair|(?i)facial|(?i)nails|(?i)lip|(?i)sunscreen|(?i)SPF|(?i)acne|(?i)pimples|(?i)scar|(?i)aging"""))\
                                  .withColumn("skincare_product",F.col("selftext").rlike("""(?i)moisturizer|(?i)cleanser|(?i)serum|(?i)toner|(?i)lotion"""))\
                                  .withColumn("skincare_product_brand",F.col("selftext").rlike("""(?i)Clinique|(?i)Neutrogena|(?i)Cetaphil|(?i)Kiehl's|(?i)Olay"""))\
                                  .withColumn("makeup",F.col("selftext").rlike("""(?i)beauty|(?i)bodypaint|(?i)cosmetics|(?i)style|(?i)artist|(?i)cosplay|(?i)fashion|(?i)celebrity|(?i)party|(?i)wedding|(?i)palette"""))\
                                  .withColumn("makeup_product",F.col("selftext").rlike("""(?i)eyeliner|(?i)contour|(?i)foundation|(?i)blush|(?i)lipstick|(?i)concealer"""))\
                                  .withColumn("makeup_product_brand",F.col("selftext").rlike("""(?i)MAC|(?i)NARS|(?i)Sephora|(?i)Fenty|(?i)Revlon|(?i)NYX|(?i)L'Oreal|(?i)Maybelline"""))\
# New submissions varaible
sampled_submissions_var= sampled_submissions_var.withColumn("is_top_100_creator",F.col("author").isin(sampled_topcreators))\
                                        .withColumn("is_peak_hours",F.col("created_hour").isin([16, 17, 18, 19, 20, 21, 22, 23, 0]))\
                                        .withColumn("is_long_text",(F.col('len') > 200))
sampled_submissions_var.cache()

# Data preparation
sampled_comments_var= sampled_comments.select("author","body","created_utc")\
                                  .withColumn("created_hour",F.hour(F.to_timestamp(F.col("created_utc").cast('int'))))\
                                  .withColumn('len',F.length(F.col('body')))
# Dummy comments varaibles
sampled_comments_var= sampled_comments_var.withColumn("skincare",F.col("body").rlike("""(?i)body|(?i)hair|(?i)facial|(?i)nails|(?i)lip|(?i)sunscreen|(?i)SPF|(?i)acne|(?i)pimples|(?i)scar|(?i)aging"""))\
                                  .withColumn("skincare_product",F.col("body").rlike("""(?i)moisturizer|(?i)cleanser|(?i)serum|(?i)toner|(?i)lotion"""))\
                                  .withColumn("skincare_product_brand",F.col("body").rlike("""(?i)Clinique|(?i)Neutrogena|(?i)Cetaphil|(?i)Kiehl's|(?i)Olay"""))\
                                  .withColumn("makeup",F.col("body").rlike("""(?i)beauty|(?i)bodypaint|(?i)cosmetics|(?i)style|(?i)artist|(?i)cosplay|(?i)fashion|(?i)celebrity|(?i)party|(?i)wedding|(?i)palette"""))\
                                  .withColumn("makeup_product",F.col("body").rlike("""(?i)eyeliner|(?i)contour|(?i)foundation|(?i)blush|(?i)lipstick|(?i)concealer"""))\
                                  .withColumn("makeup_product_brand",F.col("body").rlike("""(?i)MAC|(?i)NARS|(?i)Sephora|(?i)Fenty|(?i)Revlon|(?i)NYX|(?i)L'Oreal|(?i)Maybelline"""))\
# New comments varaibles
sampled_comments_var= sampled_comments_var.withColumn("is_top_100_creator",F.col("author").isin(sampled_topcreators))\
                                  .withColumn("is_long_text",(F.col('len') > 200))

sampled_comments_var.cache()
# Group by and count variables in submissions
sampled_submissions_peak_count= sampled_submissions_var.groupby('is_peak_hours').count().withColumn('is_peak_hours',F.col('is_peak_hours').cast('string')).toPandas()
sampled_submissions_top_count= sampled_submissions_var.groupby('is_top_100_creator').count().withColumn('is_top_100_creator',F.col('is_top_100_creator').cast('string')).toPandas()
sampled_submissions_long_count= sampled_submissions_var.groupby('is_long_text').count().withColumn('is_long_text',F.col('is_long_text').cast('string')).toPandas()
sampled_submissions_skincare_count= sampled_submissions_var.groupby('skincare').count().withColumn('skincare',F.col('skincare').cast('string')).toPandas()
sampled_submissions_makeup_count= sampled_submissions_var.groupby('makeup').count().withColumn('makeup',F.col('makeup').cast('string')).toPandas()
sampled_submissions_var_count= pd.concat([sampled_submissions_peak_count,sampled_submissions_top_count,sampled_submissions_top_count,sampled_submissions_skincare_count,sampled_submissions_makeup_count], axis=1)

# Group by and count variables in comments
sampled_comments_top_count= sampled_comments_var.groupby('is_top_100_creator').count().withColumn('is_top_100_creator',F.col('is_top_100_creator').cast('string')).toPandas()
sampled_comments_long_count= sampled_comments_var.groupby('is_long_text').count().withColumn('is_long_text',F.col('is_long_text').cast('string')).toPandas()
sampled_comments_skincare_count= sampled_comments_var.groupby('skincare').count().withColumn('skincare',F.col('skincare').cast('string')).toPandas()
sampled_comments_makeup_count= sampled_comments_var.groupby('makeup').count().withColumn('makeup',F.col('makeup').cast('string')).toPandas()
sampled_comments_var_count= pd.concat([sampled_comments_top_count,sampled_comments_long_count,sampled_comments_skincare_count,sampled_comments_makeup_count], axis=1)