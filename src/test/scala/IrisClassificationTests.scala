import com.testkraft.classifier.{Iris, IrisWrapper, Predict}
import org.scalatest._

class IrisClassificationTests extends FlatSpec with Matchers with IrisWrapper {

  val test1inputdata: String = getClass.getClassLoader.getResource("testForIrisSentosa.csv").toString

  import spark.implicits._

"A testForIrisSentosa.csv" should "evaluate to Iris-sentosa" in {

   assert(Predict.predictInputData(test1inputdata).select("namedPrediction").as[String].collect()(0) === "Iris-Setosa")


}

}
