from pyspark.sql import SparkSession

def read_csv(spark, file_path):
    """
    Read CSV file into a Spark DataFrame.
    """
    return spark.read.option("header", True).option("inferSchema", True).csv("C:\\Users\\Hp\\Downloads\\Telegram_Desktop\\Aidetic Ass\\database.csv")


def filter_earthquakes(df):
    """
    Filter earthquakes with magnitude greater than 5.0.
    """
    return df.filter(df["Magnitude"] > 5.0)

def select_columns(df):
    """
    Select Latitude, Longitude, and Magnitude columns.
    """
    return df.select("Latitude", "Longitude", "Magnitude")
