from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import LogisticRegression
from pyspark.ml import Pipeline
from pyspark.sql.functions import isnan, when, count, col


from pyspark.ml.feature import StringIndexer

def train_model():
    spark = SparkSession.builder.appName("AirlineMLModel").getOrCreate()
    data = spark.read.csv("Take_exam_data/flights.csv", header=True, inferSchema=True)
    # Load cleaned data
    
     # Convertir la colonne "Carrier" en un index numérique
    indexer = StringIndexer(inputCol="Carrier", outputCol="CarrierIndex")
    data = indexer.fit(data).transform(data)


    from pyspark.ml.regression import LinearRegression
   # Sélectionner les colonnes nécessaires et renommer "arrDelay" en "features"
    feature_col = ["DayofMonth", "DayOfWeek", "CarrierIndex", "OriginAirportID", "DestAirportID", "DepDelay"]
    data = data.select(feature_col + [(when(col("ArrDelay") > 15, 1).otherwise(0)).alias("label")])

    assembler = VectorAssembler(inputCols=feature_col, outputCol="features")
    df = assembler.transform(data)
    train_set,test_set=df.randomSplit([0.8,0.2])
    linreg=LinearRegression(featuresCol='features',labelCol='label')
    linreg=linreg.fit(train_set)



    linreg.coefficients
# Train results:
    res=linreg.evaluate(train_set)
    preds_test=res.predictions.withColumn('prediction',when(col('prediction')>0.5 ,1).otherwise(0))
    preds_test.show()
    


    

# Test results:
    res=linreg.evaluate(test_set)
    preds_train=res.predictions.withColumn('prediction',when(col('prediction')>0.5 ,1).otherwise(0))
    preds_train.show()


# Calculate confusion matrix for train set
    train_confusion_matrix = preds_train.groupBy("label", "prediction").count()
    train_confusion_matrix.show()

# Calculate confusion matrix for test set
    test_confusion_matrix = preds_test.groupBy("label", "prediction").count()
    test_confusion_matrix.show()



# Calculate precision and recall for train set
    tp = train_confusion_matrix.filter((col("label") == 1) & (col("prediction") == 1)).first()["count"]
    fp = train_confusion_matrix.filter((col("label") == 0) & (col("prediction") == 1)).first()["count"]
    fn = train_confusion_matrix.filter((col("label") == 1) & (col("prediction") == 0)).first()["count"]
    tn = train_confusion_matrix.filter((col("label") == 0) & (col("prediction") == 0)).first()["count"]

    precision_train = tp / (tp + fp)
    recall_train = tp / (tp + fn)
    accurasy_train=( tp + tn) / ( tp + fp + fn + tn)

    print("Train Accurasy:", accurasy_train)
    print("Train Precision:", precision_train)
    print("Train Recall:", recall_train)



# Calculate precision and recall for test set
    tp = test_confusion_matrix.filter((col("label") == 1) & (col("prediction") == 1)).first()["count"]
    fp = test_confusion_matrix.filter((col("label") == 0) & (col("prediction") == 1)).first()["count"]
    fn = test_confusion_matrix.filter((col("label") == 1) & (col("prediction") == 0)).first()["count"]
    tn = test_confusion_matrix.filter((col("label") == 0) & (col("prediction") == 0)).first()["count"]

    precision_test = tp / (tp + fp)
    recall_test = tp / (tp + fn)
    accurasy_test=( tp + tn) / ( tp + fp + fn + tn)

    print("Test Accurasy:", accurasy_test)
    print("Test Precision:", precision_test)
    print("Test Recall:", recall_test)


    from pyspark.ml.classification import DecisionTreeClassifier
    dt = DecisionTreeClassifier(featuresCol = 'features', labelCol = 'label')
    dtModel = dt.fit(train_set)
    predictions = dtModel.transform(test_set)
    predictions.select('label', 'rawPrediction', 'prediction', 'probability').show(10)

# Evaluate Decision Tree
    
    from pyspark.ml.evaluation import BinaryClassificationEvaluator
    from pyspark.ml.evaluation import MulticlassClassificationEvaluator
    evaluator = BinaryClassificationEvaluator()
    print("Test Area Under ROC: " + str(evaluator.evaluate(predictions, {evaluator.metricName: "areaUnderROC"})))

    evaluator = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="accuracy")
    print('Accuracy', evaluator.evaluate(predictions))
   
    # Save the model
    #model.save("data/ml_model")

    spark.stop()



if __name__ == "__main__":
    train_model()