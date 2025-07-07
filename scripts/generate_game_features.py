import sys
import os
import argparse
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws
from pyspark.sql.types import ArrayType, FloatType
from sentence_transformers import SentenceTransformer
from utils import get_logger

logger = get_logger("generate_game_features")

def encode_texts(texts, model_name="all-MiniLM-L6-v2"):
    model = SentenceTransformer(model_name)
    return model.encode(texts, show_progress_bar=True).tolist()

def main(run_id):
    try:
        spark = SparkSession.builder.appName("SteamFeatureEngineering").getOrCreate()

        JOINED_BASE = f"C:/Users/azma_/steam-game-recommender-pipeline/data/joined/{run_id}/games"
        FEATURE_BASE = f"C:/Users/azma_/steam-game-recommender-pipeline/data/featurestore/{run_id}/game_features"
        os.makedirs(FEATURE_BASE, exist_ok=True)

        logger.info(f"Loading joined games from: {JOINED_BASE}")

        # Step 1: Load joined data
        try:
            joined_df = spark.read.format("delta").load(JOINED_BASE)
        except Exception as e:
            logger.error(f"Failed to load joined data: {e}")
            raise

        # Step 2: Prepare input text
        try:
            games_text_df = joined_df.select(
                "app_id",
                "description",
                concat_ws(", ", col("tags")).alias("tags_str")
            )
            games_pd = games_text_df.toPandas()
            games_pd["description"] = games_pd["description"].fillna("").str.strip()
            games_pd["tags_str"] = games_pd["tags_str"].fillna("").str.strip()
        except Exception as e:
            logger.error(f"Failed to process input text: {e}")
            raise

        # Step 3: Generate embeddings
        try:
            logger.info("Generating embeddings...")
            games_pd["desc_emb"] = encode_texts(games_pd["description"])
            games_pd["tags_emb"] = encode_texts(games_pd["tags_str"])
        except Exception as e:
            logger.error(f"Failed to generate embeddings: {e}")
            raise

        # Step 4: Convert to Spark
        try:
            features_df = spark.createDataFrame(games_pd[["app_id", "desc_emb", "tags_emb"]])
            features_df = features_df \
                .withColumn("desc_emb", col("desc_emb").cast(ArrayType(FloatType()))) \
                .withColumn("tags_emb", col("tags_emb").cast(ArrayType(FloatType())))
        except Exception as e:
            logger.error(f"Failed to convert back to Spark: {e}")
            raise

        # Step 5: Write to Delta
        try:
            features_df.write.mode("overwrite").format("delta").save(FEATURE_BASE)
            logger.info("Feature store written to Delta successfully.")
        except Exception as e:
            logger.error(f"Failed to write features: {e}")
            raise

    except Exception as pipeline_error:
        logger.error(f"Feature engineering pipeline failed: {pipeline_error}")
        sys.exit(1)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--run_id", required=True, help="Run identifier in format YYYY-MM-DDTHH-MM-SS")
    args = parser.parse_args()

    main(args.run_id)
