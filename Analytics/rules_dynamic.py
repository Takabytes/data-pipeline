from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
import mlflow
import mlflow.spark
import os

def create_model(spark, offencesDF):
    with mlflow.start_run(run_name='random-forest'):
        #Split data
        trainDF, testDF = offencesDF.randomSplit([.8, .2], seed=42)
        #Create features column
        vecAssembler = VectorAssembler(inputCols=['Age'],
                                       outputCol='features')
        #Train the model
        rf = RandomForestClassifier(labelCol='Condamné', featuresCol='features',
                                    maxBins=40, maxDepth=5, numTrees=100, seed=42)
        mlflow.log_param('num_trees', rf.getNumTrees())
        mlflow.log_param('max_depth', rf.getMaxDepth())
        pipeline = Pipeline(stages = [vecAssembler, rf])

        #Log model
        pipelineModel = pipeline.fit(trainDF)
        mlflow.spark.log_model(
            spark_model=pipelineModel,
            artifact_path='rf-model',
            registered_model_name='convictions-detector'
        )
        #Evaluate the model
        predDF = pipelineModel.transform(testDF)
        evaluator = BinaryClassificationEvaluator(
            rawPredictionCol='rawPrediction',
            labelCol='Condamné'
        )
        performance = evaluator.evaluate(predDF)
        mlflow.log_metrics({'accuracy': performance})
        if 'ml_data.parquet' not in list(os.listdir('.')):
            offencesDF.write.parquet('ml_data.parquet')
            mlflow.log_artifact('ml_data.parquet')

def predict_on(newDF):
    model_name = 'convictions-detector'
    model_version = 1
    loaded_model = mlflow.spark.load_model(
        model_uri=f'models:/{model_name}/{model_version}'
    )
    return loaded_model.transform(newDF)


#Test
"""spark = (SparkSession
         .builder
         .appName('Random Forest model')
         .getOrCreate())

offencesDF = (spark
              .read
              .load('../data/convictions.csv', format='csv', inferSchema='true', header='true'))

create_model(spark, offencesDF)


ar = predict_on(offencesDF)
ar.show(10)"""
