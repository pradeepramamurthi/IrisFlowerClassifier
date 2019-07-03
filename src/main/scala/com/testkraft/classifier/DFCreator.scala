package com.testkraft.classifier
import org.apache.spark.sql.{DataFrame, SparkSession}

/*
   Generic Object to create and return DataFrames.
 */

object DFCreator extends IrisWrapper {




  //create a DF by taking in the path of the Iris dataset
  def buildDF(dataset: String): DataFrame = {
    val resultbuildDF = spark.read.option("inferSchema",true).option("header",true).format("csv").load(dataset).toDF()
    resultbuildDF
  }

  //create a DF by taking in the path of the Iris dataset
  def buildTrainDF(dataset: String): DataFrame = {

    val TrainDF = spark.read.option("header",true).schema(Iris.mySchema).format("csv").load(dataset).toDF()
    val resultTrainDF = TrainDF.na.fill(0.0).na.replace(Seq("SepalLength","SepalWidth","PetalLength","PetalWidth","Species"), Map("" -> "0.0"))
    resultTrainDF


  }

  //return the assembled DF with additional features column (assemble only the feature set of dataframe )
  def assembleDF(dataframe: DataFrame): DataFrame = {
    val resultassembledDF = Iris.assembler.transform(dataframe)
    resultassembledDF
  }


  //Return DF with Species column Indexed.Note: StringIndexer encodes a string column of labels to a column of label indices. The indices are in [0, numLabels), ordered by label frequencies, so the most frequent label gets index 0.
  def indexedDF(dataframe: DataFrame): DataFrame = {
    val resultindexedDF = Iris.stringIndexer.fit(dataframe).transform(dataframe)
    resultindexedDF
  }

}
