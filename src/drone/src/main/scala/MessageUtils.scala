import com.google.gson._


object MessageUtils {

  case class Message (
                       id : String,
                       location : String,
                       Date : Long,
                       Citizens : List[Citizen],
                       Words : List[String]
                     )

  case class Citizen(
                    Name : String,
                    FirstName : String,
                    Login : String,
                    PeaceScore : Int
                    )

  def parseFromJson(lines:Iterator[String]):List[Citizen] = {
    val gson = new Gson()
    lines.map(line => gson.fromJson(line, classOf[Citizen])).toList
  }
}
