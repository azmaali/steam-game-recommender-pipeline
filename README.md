# Steam Game Recommender Pipeline

This project is a hybrid recommendation pipeline for Steam games, built using PySpark and Databricks. It combines collaborative filtering and content-based filtering using structured game metadata, user interactions, and natural language features.

> Note: This repository focuses on the data pipeline and orchestration logic only.  
> For detailed exploratory notebooks, model experimentation, and visualizations, see the companion repo:  
> [Steam-Game-Recommender](https://github.com/azmaali/Steam-Game-Recommender)
## Features

- Modular ETL pipeline using Airflow
- Data cleaning, joining, and transformation on 50M+ records
- Custom feature store for content-based signals
- Sentence-transformer embeddings from game descriptions and tags
- Delta Lake format for scalable data versioning
- Pipeline organized across Databricks notebooks and PySpark scripts

## Dataset

This project uses the [Steam Game Recommendations dataset](https://www.kaggle.com/datasets/antonkozyriev/game-recommendations-on-steam) from Kaggle.

**Due to file size, the raw dataset is not included in this repository.**  
To run the pipeline locally, download the dataset from Kaggle and place it in the following folder:

data/raw/recommendations.csv
data/raw/games.csv
data/raw/games_metadata.json
data/raw/users.csv

## Tech Stack

- PySpark
- Databricks (with Unity Catalog)
- Apache Airflow
- Delta Lake
- SentenceTransformers (BERT)

## Project Structure
```
notebooks/
├── 01_Data_Exploration

data/
└── raw/ # Place Kaggle files here
└── cleaned/
└── joined/
└── features/

```

## Status

- ✅ ETL pipeline implemented and running via Airflow
- ✅ Feature engineering with custom embeddings completed
- 🔧 Currently working on model training and evaluation

## License
MIT
