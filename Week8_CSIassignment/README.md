# 🚕 Week 8 – NYC Taxi Data PySpark Project

This project centers around analyzing real-world NYC Yellow Taxi trip data using **PySpark** within a **Jupyter Notebook environment**. The main goal is to read CSV data, apply transformations, and execute advanced DataFrame queries to extract insights.

## [Notebook](./week8.ipynb)

## 📁 Dataset

- Source: [NYC Taxi & Limousine Commission](https://www.kaggle.com/datasets/gauravpathak1789/yellow-tripdata-2020-01)  
- File Used: `yellow_tripdata_2020-01.csv`  
- Size: ~593 MB+  
- Columns include fare, tips, taxes, pickup/dropoff timestamps and locations.

## ⚙️ Tech Stack

- Python  
- PySpark (Spark 3.x)  
- Jupyter Notebook  
- Parquet file format

## 📌 Steps Performed

1. **Loaded CSV** into a PySpark DataFrame from local storage  
2. **Data Cleaning**
   - Converted string timestamps to proper datetime types  
   - Cast numeric columns to appropriate data types  
3. **Saved Cleaned Data as Parquet**
   - Exported in optimized columnar format

## 🔍 Key Queries

| Query | Description |
|-------|-------------|
| Q1 | Created a `Revenue` column combining multiple monetary fields |
| Q2 | Counted total passengers grouped by pickup zone (`PULocationID`) |
| Q3 | Computed average fare and total earnings grouped by vendor |
| Q4 | Rolling count of payments by `payment_type` |
| Q5 | Identified top 2 revenue-generating vendors on a chosen date with passenger and distance stats |
| Q6 | Found the busiest route by number of passengers (`PULocationID` → `DOLocationID`) |
| Q7 | Listed top pickup zones with highest passenger count in the last 5–10 seconds (via simulated streaming) |

## 📝 Output Format

- Final DataFrame saved as:
```
yellow_taxi_2020_01_parquet/
```

- To read it back in PySpark:
```python
df = spark.read.parquet("yellow_taxi_2020_01_parquet/")
```
