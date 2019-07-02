package com.testkraft.classifier
/*
author: Pradeep Ramamurthy
 */

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.ml.feature._
import org.apache.spark.sql.types._

import org.apache.log4j.{Level, Logger}





//Create a trait that makes SparkSession available to all classes that extend this trait.

trait IrisWrapper {
  protected lazy val spark: SparkSession = SparkSession.builder().appName("IrisClass").master("local").getOrCreate()
}


/*
Object containing the main method - Entry Point.
 */

object Iris extends IrisWrapper {



  var learndataSet: String = _
  var inputdataset: String = _

  //custom Schema
  val mySchema = new StructType()
    .add("SepalLength","double")
    .add("SepalWidth","double")
    .add("PetalLength","double")
    .add("PetalWidth","double")

  //assembler for Iris features
  val assembler = new VectorAssembler()
    .setHandleInvalid("skip")
    .setInputCols(Array("SepalLength","SepalWidth","PetalLength","PetalWidth"))
    .setOutputCol("features")

  //Create Index for species column
  val stringIndexer = new StringIndexer()
    .setInputCol("Species")
    .setOutputCol("SpeciesInd")



  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.WARN)


    learndataSet = getClass.getClassLoader.getResource("learniris.csv").toString
    inputdataset = getClass.getClassLoader.getResource("TestIris.csv").toString


    //build and save the Model using learning data set - comment this out if the model doesn't have to be recomputed again and again.
    //Model.learnAndSaveModel(learndataSet)

    Predict.predictInputData(inputdataset).show()




  }





}