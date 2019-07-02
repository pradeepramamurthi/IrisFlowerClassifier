package com.testkraft.classifier
import org.apache.spark.ml.classification.{RandomForestClassifier}

/*
Creates training dataframe from the learning dataset and uses that to create a RF model and save the model
 */

object Model {


  def learnAndSaveModel(learndataset: String): Unit = {

    //Create a dataframe from the learning data
    val learnerDF = DFCreator.buildDF(Iris.learndataSet)


    //Manipulate the dataframe as needed
    val trainDataDF = DFCreator.indexedDF(DFCreator.assembleDF(learnerDF)).drop("SepalLength").drop("SepalWidth").drop("PetalLength").drop("PetalWidth")
    //println("trainDataDF")
    //trainDataDF.show()


    //Create a model using Spark ML's Random Forest Classifier applied on the trained DF (trainDataDF)
    val rfmodel = new RandomForestClassifier().setFeaturesCol("features").setLabelCol("SpeciesInd").setFeatureSubsetStrategy("sqrt").setNumTrees(15).fit(trainDataDF)


    //save the model - you can comment out the model creation above after the model has been saved, so you can re use the saved model to do the prediction.
    rfmodel.write.overwrite().save("sample-Irismodel")

  }
}
