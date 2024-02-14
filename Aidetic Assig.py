from pyspark.sql import SparkSession
from pyspark.sql.functions import concat, col, unix_timestamp, avg, udf, lit
from pyspark.sql.types import StringType
import folium
# Create SparkSession
spark = SparkSession.builder.appName("Earthquake Map").master("local").getOrCreate()
# Read the data from CSV
df = spark.read.option("header", True).option("inferSchema", True).csv("C:\\Users\\Hp\\Downloads\\Telegram_Desktop\\Aidetic Ass\\database.csv")
# Convert Date and Time columns to Timestamp
df = df.withColumn("Timestamp", unix_timestamp(concat(col("Date"), lit(" "), col("Time")), "MM/dd/yyyy HH:mm:ss").cast("timestamp"))
# Filter earthquakes with magnitude greater than 5.0
df = df.filter(col("Magnitude") > 5.0)
# Select Latitude, Longitude, and Magnitude columns
df = df.select("Latitude", "Longitude", "Magnitude")
# Convert DataFrame to Pandas DataFrame
pandas_df = df.toPandas()

# Create a map centered at a specific location
m = folium.Map(location=[0, 0], zoom_start=2)

# Add markers for each earthquake location
for index, row in pandas_df.iterrows():
    folium.Marker([row['Latitude'], row['Longitude']], popup=f"Magnitude: {row['Magnitude']}").add_to(m)

# Save the map as an HTML file
m.save("earthquake_map.html")
