import zio._
import zio.http._

object RepoExample extends ZIOAppDefault {

  val app: HttpApp[Any] =
    Routes(
      Method.GET / "text" -> handler(Response.text("Hello World!"))
    ).toHttpApp

  override val run =
    Server.serve(app).provide(Server.default)
}