import com.google.gson.Gson

object Message {
  case class Message (
                       id : String,
                       location : String,
                       Date : Long,
                       Citizens : Array[Citizen],
                       Words : Array[String]
                     )

  case class Citizen(
                      Name : String,
                      FirstName : String,
                      Login : String,
                      PeaceScore : Int
                    )

  def parseFromJson(message: String): Message = {
    val gson = new Gson()
    gson.fromJson(message, classOf[Message])
  }
}
