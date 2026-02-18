from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, lit, min, max, row_number
from pyspark.sql.window import Window


if __name__ == "__main__":
    spark = (
        SparkSession.builder
        .appName("spark-3.5-acryl-datahub")
        .config("spark.extraListeners", "datahub.spark.DatahubSparkListener")
        .config("spark.datahub.rest.server", "http://localhost:9090")
        .config("spark.datahub.metadata.dataset.materialize", "true")
        .config("spark.datahub.capture_spark_plan", "true")
        .getOrCreate()
    )

    GCP_PROJECT = "iobruno-gcp-labs"
    STG_DATASET = "stg_hackernews_rss"

    # Read staging tables
    df_hot = (
        spark.read
        .format("bigquery")
        .option("table", f"{GCP_PROJECT}.{STG_DATASET}.stg_hot_articles")
        .option("viewsEnabled", "true")
        .option("materializationDataset", STG_DATASET)
        .load()
    )
    print(f"Loaded {df_hot.count()} hot articles")

    df_newest = (
        spark.read
        .format("bigquery")
        .option("table", f"{GCP_PROJECT}.{STG_DATASET}.stg_newest_articles")
        .option("viewsEnabled", "true")
        .option("materializationDataset", STG_DATASET)
        .load()
    )
    print(f"Loaded {df_newest.count()} newest articles")

    # Tag and combine
    df_hot_tagged = df_hot.withColumn("source_feed", lit("hot"))
    df_newest_tagged = df_newest.withColumn("source_feed", lit("newest"))
    df_articles = df_hot_tagged.unionByName(df_newest_tagged)
    print(f"Total articles: {df_articles.count()}")

    # Write to BigQuery
    TARGET_DATASET = "hackernews_rss"
    TARGET_TABLE = f"{GCP_PROJECT}.{TARGET_DATASET}.articles_spark"

    (
        df_articles.write
        .format("bigquery")
        .option("table", TARGET_TABLE)
        .option("writeMethod", "direct")
        .mode("overwrite")
        .save()
    )
    print(f"Written to {TARGET_TABLE}")

    # 1. Deduplicated articles — Window function
    window_spec = Window.partitionBy("uid").orderBy(col("published_at").desc())
    df_deduped = (
        df_articles
        .withColumn("row_num", row_number().over(window_spec))
        .filter(col("row_num") == 1)
        .drop("row_num")
    )

    DEDUPED_TABLE = f"{GCP_PROJECT}.{TARGET_DATASET}.articles_deduped_spark"
    (
        df_deduped.write
        .format("bigquery")
        .option("table", DEDUPED_TABLE)
        .option("writeMethod", "direct")
        .mode("overwrite")
        .save()
    )
    print(f"Written to {DEDUPED_TABLE}")

    # 2. Top posters — Aggregation
    df_top_posters = (
        df_articles
        .groupBy("username", "source_feed")
        .agg(
            count("*").alias("article_count"),
            min("published_at").alias("earliest_published_at"),
            max("published_at").alias("latest_published_at"),
        )
    )

    TOP_POSTERS_TABLE = f"{GCP_PROJECT}.{TARGET_DATASET}.top_posters_spark"
    (
        df_top_posters.write
        .format("bigquery")
        .option("table", TOP_POSTERS_TABLE)
        .option("writeMethod", "direct")
        .mode("overwrite")
        .save()
    )
    print(f"Written to {TOP_POSTERS_TABLE}")

    # 3. Cross-posted articles — Inner join
    df_cross_posted = (
        df_hot.join(df_newest, on="uid", how="inner")
        .select(
            df_hot["uid"],
            df_hot["title"],
            df_hot["username"],
            df_hot["url"],
            df_hot["redirect_url"],
            df_hot["published_at"].alias("hot_published_at"),
            df_newest["published_at"].alias("newest_published_at"),
        )
    )

    CROSS_POSTED_TABLE = f"{GCP_PROJECT}.{TARGET_DATASET}.cross_posted_spark"
    (
        df_cross_posted.write
        .format("bigquery")
        .option("table", CROSS_POSTED_TABLE)
        .option("writeMethod", "direct")
        .mode("overwrite")
        .save()
    )
    print(f"Written to {CROSS_POSTED_TABLE}")

    # 4. Domain stats — Spark SQL with CTE
    df_articles.createOrReplaceTempView("articles")

    df_domain_stats = spark.sql("""
        WITH article_domains AS (
            SELECT
                parse_url(redirect_url, 'HOST') AS domain,
                username,
                published_at
            FROM articles
        )
        SELECT
            domain,
            count(*)                AS article_count,
            count(DISTINCT username) AS unique_authors,
            min(published_at)       AS earliest_published_at,
            max(published_at)       AS latest_published_at
        FROM article_domains
        GROUP BY domain
    """)

    DOMAIN_STATS_TABLE = f"{GCP_PROJECT}.{TARGET_DATASET}.domain_stats_spark"
    (
        df_domain_stats.write
        .format("bigquery")
        .option("table", DOMAIN_STATS_TABLE)
        .option("writeMethod", "direct")
        .mode("overwrite")
        .save()
    )
    print(f"Written to {DOMAIN_STATS_TABLE}")

    spark.stop()
