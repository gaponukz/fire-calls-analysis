import argparse

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, desc, format_string, rank, to_date, year

parser = argparse.ArgumentParser()
parser.add_argument("--input", required=True)
parser.add_argument("--output", required=True)

args = parser.parse_args()
assert args.input.endswith(".csv")

spark: SparkSession = SparkSession.builder.appName("Incident analysis").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

df = spark.read.csv(args.input, header=True, inferSchema=True)

df = df.withColumn("CallDate", to_date(col("CallDate"), "MM/dd/yyyy"))
df = df.withColumn("Year", year(col("CallDate")))

incidents_per_year = df.groupBy("Year").count().withColumnRenamed("count", "Incidents")

window = Window.partitionBy("Year").orderBy(desc("count"))
calltype_counts = df.groupBy("Year", "CallType").count()

most_popular_calltype = (
    calltype_counts.withColumn("rank", rank().over(window))
    .filter(col("rank") == 1)
    .drop("rank")
)

merged = incidents_per_year.join(most_popular_calltype, on="Year", how="left")

result = merged.select(
    merged["Year"],
    merged["Incidents"].alias("Total incidents"),
    merged["CallType"].alias("Most popular CallType"),
    format_string(
        "%d%%", ((merged["count"] * 100) / merged["Incidents"]).cast("int")
    ).alias("percent"),
).orderBy(col("Year").desc())


if __name__ == "__main__":
    result.coalesce(1).write.csv(args.output, header=True, mode="overwrite")
