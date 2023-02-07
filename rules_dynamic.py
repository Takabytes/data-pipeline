from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.tuning import ParamGridBuilder
from pyspark.ml.tuning import CrossValidator

# Init spark
spark = (SparkSession
         .builder
         .appName("Supervised model")
         .getOrCreate())

# Load data
offencesDF = (spark
              .read
              .load("local_data/convictions.csv", format='csv', inferSchema="true", header="true")
              .select('Age', 'is_sentenced'))

# Split data
trainDF, testDF = offencesDF.randomSplit([.8, .2], seed=42)

# Create features column
vecAssembler = VectorAssembler(inputCols=['Age'],
                               outputCol='features')

# Modeling step
rf = RandomForestClassifier(labelCol='is_sentenced', featuresCol='features')
pipeline = Pipeline(stages = [vecAssembler, rf])
pipelineModel = pipeline.fit(trainDF)

# Testing the model
predictions = pipelineModel.transform(testDF)
evaluator = BinaryClassificationEvaluator(
    rawPredictionCol ="rawPrediction",
    labelCol="is_sentenced"
)

print(f"Baseline model performance: {evaluator.evaluate(predictions)}")

# Hyperparameter tuning
paramGrid = (ParamGridBuilder()
             .addGrid(rf.numTrees, [2, 5, 8, 10])
             .addGrid(rf.maxDepth, [2, 3, 4, 5])
             .build())

cv = CrossValidator(estimator=rf,
                    evaluator=evaluator,
                    estimatorParamMaps=paramGrid,
                    numFolds=4,
                    seed=42)

newPipeline = Pipeline(stages = [vecAssembler, cv])
newPipelineModel = newPipeline.fit(trainDF)

newPredictions = newPipelineModel.transform(testDF)
print(f"Tuned model performance: {evaluator.evaluate(newPredictions)}")


pipelinePath = "./rf-pipeline-model"
newPipelineModel.write().overwrite().save(pipelinePath)
