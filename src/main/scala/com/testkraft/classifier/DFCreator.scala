package com.testkraft.classifier
import org.apache.spark.sql.{DataFrame, SparkSession}

/*
   Generic Object to create and return DataFrames.
 */

object DFCreator extends IrisWrapper {




  //create a DF by taking in the path of the Iris dataset
  def buildDF(dataset: String): DataFrame = {
    spark.read.option("inferSchema",true).option("header",true).format("csv").load(dataset).toDF()
  }

  //create a DF by taking in the path of the Iris dataset
  def buildTrainDF(dataset: String): DataFrame = {

    spark.read.option("header",true).schema(Iris.mySchema).format("csv").load(dataset).toDF()

  }

  //return the assembled DF with additional features column (assemble only the feature set of dataframe )
  def assembleDF(dataframe: DataFrame): DataFrame = {
    Iris.assembler.transform(dataframe)
  }


  //Return DF with Species column Indexed.Note: StringIndexer encodes a string column of labels to a column of label indices. The indices are in [0, numLabels), ordered by label frequencies, so the most frequent label gets index 0.
  def indexedDF(dataframe: DataFrame): DataFrame = {
    Iris.stringIndexer.fit(dataframe).transform(dataframe)
  }

}
