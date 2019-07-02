package com.testkraft.classifier

import org.apache.spark.ml.classification.RandomForestClassificationModel
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object Predict extends IrisWrapper {

/*
    Takes path to a test dataset and returns prediction result as a DF after applying the saved RF Model.
 */

  def predictInputData(inputdataset: String): DataFrame = {

    //Create a dataframe from the input data
    val testerDF = DFCreator.buildTrainDF(inputdataset)


    //Manipulate the dataframe as needed
    val testDataDF = DFCreator.assembleDF(testerDF).drop("SepalLength").drop("SepalWidth").drop("PetalLength").drop("PetalWidth")



    //load the saved model and apply to the data frame that needs to be tested (testDataDF)
    val prediction_TestIris = RandomForestClassificationModel.load("Sample-IrisModel").transform(testDataDF).toDF()



    // Add an additional column to make the prediction column human readable.
    val predictionResultDF = prediction_TestIris.withColumn("namedPrediction",
      when(col("prediction") === "0.0", "Iris-Setosa").otherwise(when(col("prediction") === "1.0", "Iris-Versicolor").otherwise(when(col("prediction") === "2.0", "Iris-Virginica"))))

    predictionResultDF


  }
}


