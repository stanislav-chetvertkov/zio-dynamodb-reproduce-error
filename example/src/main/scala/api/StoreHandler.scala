package api

import api.Protocol.CreateUser
import api.StoreResource.GetUserByIdResponse
import service.ConfigurationService.User
import zio.*
import zio.http.*
import zio.json.{DecoderOps, DeriveJsonDecoder, EncoderOps, JsonDecoder}


object Protocol {
  case class CreateUser(region: String, code: String, parent: String)

  object CreateUser {
    implicit val decoder: JsonDecoder[CreateUser] = DeriveJsonDecoder.gen[CreateUser]
  }
  //  case class GetUserResponse(id: String, region: String, code: String, parent: String)
}

trait StoreHandler {
  def getOrderById(respond: StoreResource.GetUserByIdResponse.type)
                  (id: Long): Task[StoreResource.GetUserByIdResponse]

  def postById(respond: StoreResource.PostUserByIdResponse.type)
              (id: Long, user: CreateUser): Task[StoreResource.PostUserByIdResponse]
}

object StoreResource {

  sealed trait Error extends Exception

  private case class DecodingError(message: String) extends Error {
    override def getMessage: String = message
  }

  case class ServiceError(message: String) extends Error

  def getUserByIdRoute(impl: StoreHandler): Route[Any, Response] = {
    val pattern: RoutePattern[Int] = Method.GET / "users" / int("id")
    pattern -> {
      Handler.fromFunctionZIO { (in: (Int, Request)) =>
        impl.getOrderById(GetUserByIdResponse)(in._1)
          .map(r => r.toResponse)
      }.mapError(e => Response.text("Error: " + e.getMessage).status(Status.InternalServerError))
    }
  }

//  Routes.fromIterable() try that as well

  def routes(impl: StoreHandler): Routes[Any, Response] = Routes(
    getUserByIdRoute(impl),
    Method.POST / "users" -> {
      val r = for {
        userCreated <- Handler.fromFunction[CreateUser] { (c: CreateUser) => c }
          .contramapZIO[Any, DecodingError, Request](req => {
            req.body.asString
              .mapError(e => DecodingError(e.getMessage))
              .flatMap(jsonString =>
                ZIO.fromEither(jsonString.fromJson[CreateUser])
                  .mapError(e => DecodingError(s"Failed to decode $jsonString:" + e))
              )
          })
        response <- Handler.fromZIO(
          for {
            userCreated <- impl.postById(PostUserByIdResponse)(42, userCreated).map(PostUserByIdResponse.postOrderByIdResponseTR)
          } yield {
            userCreated
          }
        )
      } yield response
      val q = r.mapError(e => Response.text("Error: " + e.getMessage).status(Status.InternalServerError))
      q
    }
  )


  sealed abstract class GetUserByIdResponse(val statusCode: Status) {
    def toResponse: Response
  }

  case class GetUserByIdResponseOK(value: User) extends GetUserByIdResponse(Status.Ok) {

    override def toResponse: Response = {
      Response(
        this.statusCode,
        Headers(Header.ContentType(MediaType.application.json).untyped),
        Body.fromCharSequence(value.toJsonPretty)
      )
    }
  }

  case object GetUserByIdResponseBadRequest extends GetUserByIdResponse(Status.BadRequest) {

    override def toResponse: Response = {
      Response(
        this.statusCode,
        Headers(Header.ContentType(MediaType.application.json).untyped),
        Body.fromCharSequence("""{"error": "Bad Request"}""")
      )
    }
  }

  case object GetUserByIdResponseNotFound extends GetUserByIdResponse(Status.NotFound) {
    override def toResponse: Response = Response(
      this.statusCode,
      Headers(Header.ContentType(MediaType.application.json).untyped),
      Body.fromCharSequence("""{"error": "Not Found"}""")
    )
  }

  object GetUserByIdResponse {

    //    def apply[T](value: T)(implicit ev: T => GetUserByIdResponse): GetUserByIdResponse = ev(value)

    //    implicit def OKEv(value: User): GetUserByIdResponse = OK(value)

    def OK(value: User): GetUserByIdResponse = GetUserByIdResponseOK(value)

    def BadRequest: GetUserByIdResponse = GetUserByIdResponseBadRequest

    def NotFound: GetUserByIdResponse = GetUserByIdResponseNotFound
  }


  /////////////////////////////////////////////////////////////

  sealed abstract class PostUserByIdResponse(val statusCode: Status)

  case class PostUserByIdResponseOK(value: User) extends PostUserByIdResponse(Status.Ok)

  case object PostUserByIdResponseNotFound extends PostUserByIdResponse(Status.NotFound)

  object PostUserByIdResponse {

    implicit def postOrderByIdResponseTR(value: PostUserByIdResponse): Response = value match {
      case r: PostUserByIdResponseOK =>
        Response(
          r.statusCode,
          Headers(Header.ContentType(MediaType.application.json).untyped),
          Body.fromCharSequence(r.value.toJsonPretty)
        )
      case r: PostUserByIdResponseNotFound.type =>
        Response(
          r.statusCode,
          Headers(Header.ContentType(MediaType.application.json).untyped),
          Body.fromCharSequence("""{"error": "Not Found"}""")
        )
    }

    def OK(value: User): PostUserByIdResponse = PostUserByIdResponseOK(value)

    def NotFound: PostUserByIdResponse = PostUserByIdResponseNotFound
  }

}
