package com.testkraft.classifier
/*
author: Pradeep Ramamurthy
 */

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.ml.feature._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.classification.{RandomForestClassifier,RandomForestClassificationModel}



//Create a trait that makes SparkSession available to all classes that extend this trait.

trait IrisWrapper {
  protected lazy val spark: SparkSession = SparkSession.builder().appName("IrisClass").master("local").getOrCreate()
}

object Iris extends IrisWrapper {



  //assembler for Iris features
  private val assembler = new VectorAssembler()
    .setHandleInvalid("skip")
    .setInputCols(Array("SepalLength","SepalWidth","PetalLength","PetalWidth"))
    .setOutputCol("features")

  //Create Index for species column
  private val stringIndexer = new StringIndexer()
    .setInputCol("Species")
    .setOutputCol("SpeciesInd")

  val mySchema = new StructType()
    .add("SepalLength","double")
    .add("SepalWidth","double")
    .add("PetalLength","double")
    .add("PetalWidth","double")




  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.WARN)


    val inputdataset = getClass.getClassLoader.getResource("TestIris.csv").toString
    val learndataSet = getClass.getClassLoader.getResource("learniris.csv").toString
    val learnerDF = buildDF(learndataSet)





    val testerDF =  buildTrainDF(inputdataset)



    val trainDataDF = indexedDF(assembleDF(learnerDF)).drop("SepalLength").drop("SepalWidth").drop("PetalLength").drop("PetalWidth")
    //println("trainDataDF")
    //trainDataDF.show()

    val testDataDF = assembleDF(testerDF).drop("SepalLength").drop("SepalWidth").drop("PetalLength").drop("PetalWidth")
    //println("testDataDF")
    //testDF.show()


    //Create a model using Spark ML's Random Forest Classifier applied on trainDataDF
    val rfmodel = new RandomForestClassifier().setFeaturesCol("features").setLabelCol("SpeciesInd").setFeatureSubsetStrategy("sqrt").setNumTrees(15).fit(trainDataDF)

    //save the model - you can comment out the model creation above after the model has been saved, so you can re use the saved model to do the prediction.
    rfmodel.write.overwrite().save("sample-Irismodel")

    //load the saved model.
     val prediction_TestIris = RandomForestClassificationModel.load("Sample-IrisModel").transform(testDataDF).toDF()



    // Add an additional column to make the prediction column human readable.
    prediction_TestIris.withColumn("namedPrediction",
      when(col("prediction") === "0.0", "Iris-setosa").otherwise(when(col("prediction") === "1.0", "Iris-versicolor").otherwise(when(col("prediction") === "2.0", "Iris-virginica")))).show()





  }


  //create a DF by taking in the path of the Iris dataset
  private def buildDF(dataset: String): DataFrame = {
    spark.read.option("inferSchema",true).option("header",true).format("csv").load(dataset).toDF()
  }

  //create a DF by taking in the path of the Iris dataset
  private def buildTrainDF(dataset: String): DataFrame = {
    spark.read.option("header",true).schema(mySchema).format("csv").load(dataset).toDF()
  }


  //assemble only the feature set of dataframe and return the assembled DF with additional features column
  private def assembleDF(dataframe: DataFrame): DataFrame = {
    assembler.transform(dataframe)
  }


  //buildDF(inputdataset).show()
  // assembleDF(buildDF(inputdataset)).show()

  //StringIndexer encodes a string column of labels to a column of label indices. The indices are in [0, numLabels), ordered by label frequencies, so the most frequent label gets index 0.
  private def indexedDF(dataframe: DataFrame): DataFrame = {
    stringIndexer.fit(dataframe).transform(dataframe)
  }


}