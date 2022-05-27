import com.google.gson._

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

  def sendPeaceMaker(str: String): Unit = {
    val message = parseFromJson(str)
    message.Citizens.filter(x => x.PeaceScore < 30).foreach(x => println("Send PeaceMaker to: " + message.location + " for: " + x.Name + " " + x.FirstName  + ", login: " + x.Login ))
  }
}
