from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.tuning import ParamGridBuilder
from pyspark.ml.tuning import CrossValidator
import mlflow
import mlflow.spark
import pandas as pd

# Init spark
spark = (SparkSession
         .builder
         .appName('Random Forest model')
         .getOrCreate())

# Load data
offencesDF = (spark
              .read
              .load('data/convictions.csv', format='csv', inferSchema='true', header='true')
              .select('Age', 'is_sentenced'))

# Split data
trainDF, testDF = offencesDF.randomSplit([.8, .2], seed=42)

# Create features column
vecAssembler = VectorAssembler(inputCols=['Age'],
                               outputCol='features')

# Modeling step
rf = RandomForestClassifier(labelCol='is_sentenced', featuresCol='features')
pipeline = Pipeline(stages = [vecAssembler, rf])

mlflow.start_run(run_name='random-forest')
# Log params
mlflow.log_param("num_trees", rf.getNumTrees())
mlflow.log_param("max_depth", rf.getMaxDepth())
# Log model
pipelineModel = pipeline.fit(trainDF)
mlflow.spark.log_model(pipelineModel, 'model')
# Log metrics
predDF = pipelineModel.transform(testDF)
evaluator = BinaryClassificationEvaluator(
    rawPredictionCol="rawPrediction",
    labelCol="is_sentenced"
)
mlflow.log_metrics({'performance_metric': evaluator.evaluate(predDF)})
#Log artifact: features importance scores
rfModel = pipelineModel.stages[-1]
pandasDF = (pd.DataFrame(list(zip(vecAssembler.getInputCols(),
                                  rfModel.featureImportances)),
                         columns=['feature', 'importance'])
            .sort_values(by='importance', ascending=False))
pandasDF.to_csv('feature-importance.csv', index=False)
mlflow.log_artifact('feature-importance.csv')
