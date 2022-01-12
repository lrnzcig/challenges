import pyspark.sql.functions as F

from pyspark.sql import DataFrame


def metrics_calculation(input_df: DataFrame) -> DataFrame:
    df_dedupl = input_df.drop_duplicates(["id"])
    df_dedupl = df_dedupl.withColumn("consent", F.size(F.col("token.purposes.enabled")) > 0)

    count_type_all = df_dedupl.groupby(['datehour', 'domain', 'user.country']).pivot('type') \
        .agg(F.count(df_dedupl.type)).withColumnRenamed('pageview', 'pageviews') \
        .withColumnRenamed('consent.asked', 'consent_asked') \
        .withColumnRenamed('consent.given', 'consent_given')

    count_type_w_consent = df_dedupl.filter(F.col('consent') == True) \
        .groupby(['datehour', 'domain', 'user.country']).pivot('type').agg(F.count(df_dedupl.type)) \
        .withColumnRenamed('pageview', 'pageviews_with_consent') \
        .withColumnRenamed('consent.asked', 'consent_asked_with_consent') \
        .withColumnRenamed('consent.given', 'consent_given_with_consent')

    avg_pageviews_p_user = df_dedupl.filter(F.col('type') == "pageview") \
        .groupby(['datehour', 'domain', 'user.country', 'user.id']).agg(F.count(df_dedupl.user.id).alias('count')) \
        .groupby(['datehour', 'domain', 'country']).agg(F.mean('count').alias('avg_pageviews_p_user'))

    return count_type_all\
        .join(count_type_w_consent, on=['datehour', 'domain', 'country'])\
        .join(avg_pageviews_p_user, on=['datehour', 'domain', 'country']).na.fill(value=0)
