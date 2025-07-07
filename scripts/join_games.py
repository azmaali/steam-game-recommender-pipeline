import sys
import os
import argparse
from pyspark.sql import SparkSession
from utils import get_logger

logger = get_logger("join_games")

def main(run_id):
    try:
        spark = SparkSession.builder.appName("JoinGames").getOrCreate()

        CLEANED_BASE = f"C:/Users/azma_/steam-game-recommender-pipeline/data/clean/{run_id}"
        JOINED_BASE = f"C:/Users/azma_/steam-game-recommender-pipeline/data/joined/{run_id}/games"
        os.makedirs(os.path.dirname(JOINED_BASE), exist_ok=True)

        logger.info(f"Joining cleaned datasets. Output: {JOINED_BASE}")

        # Step 1: Load cleaned data
        try:
            games_df = spark.read.format("delta").load(f"{CLEANED_BASE}/games")
            metadata_df = spark.read.format("delta").load(f"{CLEANED_BASE}/games_metadata")
            logger.info("Loaded cleaned Delta tables.")
        except Exception as e:
            logger.error(f"Failed to load cleaned data: {e}")
            raise

        # Step 2: Join
        try:
            joined_df = games_df.join(
                metadata_df.select("app_id", "description", "tags"),
                on="app_id",
                how="left"
            )
            logger.info(f"Join completed. Row count: {joined_df.count()}")
        except Exception as e:
            logger.error(f"Join failed: {e}")
            raise

        # Step 3: Write to Delta
        try:
            joined_df.write.mode("overwrite").format("delta").save(JOINED_BASE)
            logger.info("Joined games data written to Delta successfully.")
        except Exception as e:
            logger.error(f"Failed to write joined data: {e}")
            raise

    except Exception as pipeline_error:
        logger.error(f"Join pipeline failed: {pipeline_error}")
        sys.exit(1)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--run_id", required=True, help="Run identifier in format YYYY-MM-DDTHH-MM-SS")
    args = parser.parse_args()

    main(args.run_id)
