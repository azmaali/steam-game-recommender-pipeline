import sys
import os
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType, ArrayType
from utils import filter_missing, encode_column, normalize_tags, get_logger


logger = get_logger("clean_data")

def main():
    try:
        spark = SparkSession.builder.appName("SteamClean").getOrCreate()

        # Timestamped run ID for versioning
        run_id = datetime.now().strftime("%Y-%m-%dT%H-%M-%S")
        RAW_BASE = "C:/Users/azma_/steam-game-recommender-pipeline/data/raw"
        CLEANED_BASE = f"C:/Users/azma_/steam-game-recommender-pipeline/data/clean/{run_id}"

        os.makedirs(CLEANED_BASE, exist_ok=True)
        logger.info(f"Output will be written to: {CLEANED_BASE}")

        # Step 1: Load raw files
        try:
            games_df = spark.read.option("header", True).csv(f"{RAW_BASE}/games.csv")
            games_metadata_df = spark.read.json(f"{RAW_BASE}/games_metadata.json")
            recommendations_df = spark.read.option("header", True).csv(f"{RAW_BASE}/recommendations.csv")
            users_df = spark.read.option("header", True).csv(f"{RAW_BASE}/users.csv")
            logger.info("Raw files loaded successfully.")
        except Exception as e:
            logger.error(f"Failed to load raw data: {e}")
            raise

        # Step 2: Filter missing values and drop duplicates
        try:
            games_df = filter_missing(games_df).dropDuplicates()
            games_metadata_df = filter_missing(games_metadata_df).dropDuplicates()
            recommendations_df = filter_missing(recommendations_df).dropDuplicates()
            users_df = filter_missing(users_df).dropDuplicates()
            logger.info("Missing values filtered and duplicates dropped.")
        except Exception as e:
            logger.error(f"Error during filtering/deduplication: {e}")
            raise

        # Step 3: Cast types
        try:
            games_df = games_df \
                .withColumn("app_id", col("app_id").cast("long")) \
                .withColumn("date_release", col("date_release").cast("date")) \
                .withColumn("win", col("win").cast("boolean")) \
                .withColumn("mac", col("mac").cast("boolean")) \
                .withColumn("linux", col("linux").cast("boolean")) \
                .withColumn("steam_deck", col("steam_deck").cast("boolean")) \
                .withColumn("positive_ratio", col("positive_ratio").cast("float")) \
                .withColumn("user_reviews", col("user_reviews").cast("int")) \
                .withColumn("price_final", col("price_final").cast("float")) \
                .withColumn("price_original", col("price_original").cast("float")) \
                .withColumn("discount", col("discount").cast("float"))

            recommendations_df = recommendations_df \
                .withColumn("app_id", col("app_id").cast("long")) \
                .withColumn("helpful", col("helpful").cast("int")) \
                .withColumn("funny", col("funny").cast("int")) \
                .withColumn("date", col("date").cast("date")) \
                .withColumn("is_recommended", col("is_recommended").cast("boolean")) \
                .withColumn("hours", col("hours").cast("float")) \
                .withColumn("user_id", col("user_id").cast("string")) \
                .withColumn("review_id", col("review_id").cast("string"))

            users_df = users_df \
                .withColumn("user_id", col("user_id").cast("string")) \
                .withColumn("products", col("products").cast("int")) \
                .withColumn("reviews", col("reviews").cast("int"))

            logger.info("Column types cast successfully.")
        except Exception as e:
            logger.error(f"Error during type casting: {e}")
            raise

        # Step 4: Normalize tags and encode rating
        try:
            normalize_tags_udf = udf(normalize_tags, ArrayType(StringType()))
            games_metadata_df = games_metadata_df.withColumn("tags", normalize_tags_udf(col("tags")))

            rating_order = [
                'Overwhelmingly Negative', 'Very Negative', 'Mostly Negative', 'Negative',
                'Mixed', 'Mostly Positive', 'Positive', 'Very Positive', 'Overwhelmingly Positive'
            ]
            rating_mapping = {rating: idx for idx, rating in enumerate(rating_order)}
            games_df = encode_column(games_df, "rating", rating_mapping)

            logger.info("Tags normalized and ratings encoded.")
        except Exception as e:
            logger.error(f"Error during tag normalization or rating encoding: {e}")
            raise

        # Step 5: Write cleaned data
        try:
            games_df.write.mode("overwrite").format("delta").save(f"{CLEANED_BASE}/games")
            games_metadata_df.write.mode("overwrite").format("delta").save(f"{CLEANED_BASE}/games_metadata")
            recommendations_df.write.mode("overwrite").format("delta").save(f"{CLEANED_BASE}/recommendations")
            users_df.write.mode("overwrite").format("delta").save(f"{CLEANED_BASE}/users")
            logger.info("Cleaned data written to Delta files successfully.")
        except Exception as e:
            logger.error(f"Failed to write cleaned data: {e}")
            raise

        logger.info("Clean pipeline completed successfully.")

    except Exception as pipeline_error:
        logger.error(f"Pipeline failed: {pipeline_error}")
        sys.exit(1)

if __name__ == "__main__":
    main()
