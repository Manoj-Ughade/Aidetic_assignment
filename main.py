from pyspark.sql.functions import concat, col, unix_timestamp, lit
import folium
from pyspark.sql import SparkSession
from src.data_processing import read_csv, filter_earthquakes, select_columns

def generate_map(df, output_file):
    """
    Generate Folium Map and save it as an HTML file.
    """
    # Convert Date and Time columns to Timestamp
    df = df.withColumn("Timestamp", unix_timestamp(concat(col("Date"), lit(" "), col("Time")), "MM/dd/yyyy HH:mm:ss").cast("timestamp"))

    # Apply data processing steps
    df = filter_earthquakes(df)
    df = select_columns(df)

    # Convert DataFrame to Pandas DataFrame
    pandas_df = df.toPandas()

    # Create a map centered at a specific location
    m = folium.Map(location=[0, 0], zoom_start=2)

    # Add markers for each earthquake location
    for index, row in pandas_df.iterrows():
        folium.Marker([row['Latitude'], row['Longitude']], popup=f"Magnitude: {row['Magnitude']}").add_to(m)

    # Save the map as an HTML file
    m.save(output_file)

if __name__ == "__main__":
    # Create SparkSession
    spark = SparkSession.builder.appName("Earthquake Map").master("local").getOrCreate()

    # Read the data from CSV
    file_path = "path/to/database.csv"
    df = read_csv(spark, file_path)

    # Generate and save the map
    output_file = "earthquake_map.html"
    generate_map(df, output_file)
