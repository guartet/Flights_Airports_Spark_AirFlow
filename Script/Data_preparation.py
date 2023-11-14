from pyspark.sql import SparkSession
from pyspark.sql.functions import *

def prepare_data():
    spark = SparkSession.builder.appName("AirlineDataPreparation").getOrCreate()

    # Load data
    data = spark.read.csv("Take_exam_data/flights.csv", header=True, inferSchema=True)
    df_airports = spark.read.csv("Take_exam_data/airports.csv", header=True, inferSchema=True)

    data.show(5)
    print(data.count())
    #print(data1.count())


    # Count of missing values for each column
    from pyspark.sql.functions import isnan, when, count, col
    data.select([count(when(col(c).isNull(),c)).alias(c) for c in data.columns]).show()

    # Supprimer les doublons
    df = data.dropDuplicates()

    df = df.filter(col("DayofMonth") <= 31)
    df = df.filter(col("DayOfWeek") <= 7)

# Gérer les valeurs aberrantes
# Exemple : Supprimer les vols avec un délai de départ négatif (impossible)
    #df = df.filter(df.DepDelay >= 0)

# Gérer les valeurs manquantes
# Exemple : Remplacer les valeurs manquantes dans ArrDelay par 0
    df = df.na.fill(0, subset=["ArrDelay"])

# Changer le type des colonnes si nécessaire
    df = df.withColumn("DepDelay", df["DepDelay"].cast("int"))
    df = df.withColumn("ArrDelay", df["ArrDelay"].cast("int"))
    print(df.columns)
    print(df_airports.columns)

    
    #  Jointure avec une autre table Airport
    
    df = df.join(df_airports, df["OriginAirportID"] == df_airports["airport_id"], "left")
    df.show(10)
    #df.tail(5)
    from pyspark.sql.functions import expr

# Calculer la durée du vol
    #df = df.withColumn("FlightDuration", df.ArrDelay + df.DepDelay).show()



#from pyspark.sql.functions import month, dayofweek

# Extraire le mois et le jour de la semaine à partir de la date
    #df = df.withColumn("DayofMonth", month("DayofMonth"))
    #df = df.withColumn("DayOfWeek", dayofweek("Date"))


# Analyse des tendances temporelles
    temporal_analysis = df.groupBy("DayofMonth", "DayOfWeek").agg(avg("ArrDelay"))
    temporal_analysis.show(10)




    data = data.select("DayofMonth", "DayOfWeek", "Carrier", "OriginAirportID", "DestAirportID", "DepDelay", ((col("ArrDelay") > 15).cast("Int").alias("label")))
    data.show(10)


    
    # Save cleaned data
    #cleaned_data.write.mode("overwrite").parquet("data/cleaned_data.parquet")

    

    spark.stop()

if __name__ == "__main__":
    prepare_data()



