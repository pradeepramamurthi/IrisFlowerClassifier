import com.testkraft.classifier.{Iris, IrisWrapper, Predict}
import org.scalatest._

class IrisClassificationTests extends FlatSpec with Matchers with IrisWrapper {

  val test1inputdata: String = getClass.getClassLoader.getResource("testForIrisSetosa.csv").toString
  val test2inputdata: String = getClass.getClassLoader.getResource("testForIrisVersicolor.csv").toString
  val test3inputdata: String = getClass.getClassLoader.getResource("testForIrisVirginica.csv").toString
  val test4inputdata: String = getClass.getClassLoader.getResource("testZeros.csv").toString
  val test5inputdata: String = getClass.getClassLoader.getResource("testEmptyRecords.csv").toString
  val test6inputdata: String = getClass.getClassLoader.getResource("testExtraRecord.csv").toString


  import spark.implicits._

"A testForIrisSetosa.csv" should "evaluate to Iris-sentosa" in {

    //use of should be from matchers
    Predict.predictInputData(test1inputdata).select("namedPrediction").as[String].collect()(0) should be ("Iris-Setosa")


  }

  "A testForIrisVersicolor.csv" should "evaluate to Iris-versicolor" in {
    //use of assert
    assert(Predict.predictInputData(test2inputdata).select("namedPrediction").as[String].collect()(0) === "Iris-Versicolor")


  }


  "A testForIrisVirginica.csv" should "evaluate to Iris-Virginica" in {

    assert(Predict.predictInputData(test3inputdata).select("namedPrediction").as[String].collect()(0) === "Iris-Virginica")


  }

  "A testZeros.csv" should "evaluate to Iris-Setosa" in {

    assert(Predict.predictInputData(test4inputdata).select("namedPrediction").as[String].collect()(0) === "Iris-Setosa")


  }


  "A testEmptyRecords.csv" should "evaluate to ???" in {

    //Checks if null and empty records are handled.
    assert(Predict.predictInputData(test5inputdata).select("namedPrediction").as[String].collect()(0) === "Iris-Setosa")


  }

  "A testExtraRecords.csv" should "evaluate to ???" in {

    //Checks behaviour if there is an extra record
    assert(Predict.predictInputData(test6inputdata).select("namedPrediction").as[String].collect()(0) === "Iris-Setosa")


  }

}
