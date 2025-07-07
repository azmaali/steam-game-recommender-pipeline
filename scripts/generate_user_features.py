import sys
import os
import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum as _sum, avg, when
from utils import get_logger

logger = get_logger("generate_user_features")

def main(run_id):
    try:
        spark = SparkSession.builder.appName("SteamUserFeatureEngineering").getOrCreate()

        CLEANED_BASE = f"C:/Users/azma_/steam-game-recommender-pipeline/data/clean/{run_id}"
        FEATURE_BASE = f"C:/Users/azma_/steam-game-recommender-pipeline/data/featurestore/{run_id}/user_features"
        os.makedirs(FEATURE_BASE, exist_ok=True)

        logger.info(f"Generating user features for run_id: {run_id}")

        # Step 1: Load cleaned datasets
        try:
            users_df = spark.read.format("delta").load(f"{CLEANED_BASE}/users")
            recommendations_df = spark.read.format("delta").load(f"{CLEANED_BASE}/recommendations")
            logger.info("Cleaned user and recommendation data loaded.")
        except Exception as e:
            logger.error(f"Failed to load input data: {e}")
            raise

        # Step 2: Aggregate recommendation behavior
        try:
            agg_df = recommendations_df.groupBy("user_id").agg(
                count("*").alias("num_reviews"),
                _sum("hours").alias("total_playtime"),
                avg("hours").alias("avg_playtime"),
                _sum(when(col("is_recommended") == True, 1).otherwise(0)).alias("num_liked"),
                count("app_id").alias("distinct_games_reviewed")
            ).withColumn(
                "liked_ratio", col("num_liked") / col("num_reviews")
            )
        except Exception as e:
            logger.error(f"Error during user aggregation: {e}")
            raise

        # Step 3: Join with user profile data
        try:
            user_features_df = users_df.join(agg_df, on="user_id", how="left") \
                .withColumn("avg_reviews_per_game", col("reviews") / col("products"))
            logger.info("User features generated successfully.")
        except Exception as e:
            logger.error(f"Join with users_df failed: {e}")
            raise

        # Step 4: Save user features
        try:
            user_features_df.write.mode("overwrite").format("delta").save(FEATURE_BASE)
            logger.info("User features saved to Delta successfully.")
        except Exception as e:
            logger.error(f"Failed to write user features: {e}")
            raise

    except Exception as pipeline_error:
        logger.error(f"User feature pipeline failed: {pipeline_error}")
        sys.exit(1)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--run_id", required=True, help="Run identifier in format YYYY-MM-DDTHH-MM-SS")
    args = parser.parse_args()

    main(args.run_id)
