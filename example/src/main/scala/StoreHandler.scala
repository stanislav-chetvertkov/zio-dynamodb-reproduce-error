import ConfigurationService.User
import Protocol.CreateSmsEndpoint
import zio._
import zio.http.Status._
import zio.http._


object Protocol {
  case class CreateSmsEndpoint(name: String)
}

trait StoreHandler {
  def getOrderById(respond: StoreResource.GetUserByIdResponse.type)
                  (id: Long): Task[StoreResource.GetUserByIdResponse]

  def postById(respond: StoreResource.PostUserByIdResponse.type)
              (id: Long): Task[StoreResource.PostUserByIdResponse]
}

object StoreResource {

  sealed trait Error extends Exception

  private case class DecodingError(message: String) extends Error {
    override def getMessage: String = message
  }

  case class ServiceError(message: String) extends Error

  def routes(impl: StoreHandler): Routes[Any, Throwable] = Routes(
    Method.GET / "users" / int("id") -> {
      val x: Handler[Any, Throwable, (RuntimeFlags, Request), Response] =
        Handler.fromFunctionZIO { in: (Int, Request) =>
          impl.getOrderById(GetUserByIdResponse)(in._1).map(GetUserByIdResponse.getOrderByIdResponseTR)
        }
      x
    },
    Method.POST / "users" / int("id") -> {
      val x = for {
        req <- Handler.fromFunctionZIO { in: (Int, Request) =>
          //decode to CreateUser

          ZIO.succeed(in._1).zip(
            in._2.body.asString.flatMap(jsonString =>
              ZIO.fromEither(User.jsonEncoder.decoder.decodeJson(jsonString))
                .mapError(e => DecodingError(s"Failed to decode $jsonString:" + e))
            )
          )
        }
        response <- Handler.fromZIO(
          for {
            r <- ZIO.succeed(req._2)
            userCreated <- impl.postById(PostUserByIdResponse)(req._1).map(PostUserByIdResponse.postOrderByIdResponseTR)
          } yield {
            userCreated
          }
        )
      } yield {
        response
      }
      x
    },
    Method.POST / "users2" / int("id") / "page" / int("count") -> {
      val x = for {
        req <- Handler.fromFunctionZIO { in: (Int, Int, Request) => ZIO.succeed(in) }

        //        userCreated <- Handler.fromFunction[CreateUser] { case CreateUser(_) => User(2)}
        //          .contramapZIO[Any, DecodingError, Request](req => {
        //            ZIO.succeed(CreateUser(req.path.encode))
        //          })
        response <- Handler.fromZIO(
          for {
            r <- ZIO.succeed(CreateSmsEndpoint("1"))
            userCreated <- impl.postById(PostUserByIdResponse)(1l).map(PostUserByIdResponse.postOrderByIdResponseTR)
          } yield {
            userCreated
          }
        )
      } yield {
        response
      }
      x
    }
  )


  sealed abstract class GetUserByIdResponse(val statusCode: Status)

  case class GetUserByIdResponseOK(value: User) extends GetUserByIdResponse(Status.Ok)

  case object GetUserByIdResponseBadRequest extends GetUserByIdResponse(Status.BadRequest)

  case object GetUserByIdResponseNotFound extends GetUserByIdResponse(Status.NotFound)

  object GetUserByIdResponse {

    implicit def getOrderByIdResponseTR(value: GetUserByIdResponse): Response = value match {
      case r: GetUserByIdResponseOK =>
        Response(
          r.statusCode,
          Headers(Header.ContentType(MediaType.application.json).untyped),
          Body.fromCharSequence("""{"error": "Not Found"}""") //fixme
        )
      case r: GetUserByIdResponseBadRequest.type =>
        Response(
          r.statusCode,
          Headers(Header.ContentType(MediaType.application.json).untyped),
          Body.fromCharSequence("""{"error": "Bad Request"}""")
        )
      case r: GetUserByIdResponseNotFound.type =>
        Response(
          r.statusCode,
          Headers(Header.ContentType(MediaType.application.json).untyped),
          Body.fromCharSequence("""{"error": "Not Found"}""")
        )
    }

    def apply[T](value: T)(implicit ev: T => GetUserByIdResponse): GetUserByIdResponse = ev(value)

    implicit def OKEv(value: User): GetUserByIdResponse = OK(value)

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

          //
          Body.fromCharSequence("""{"error": "Not Found"}""") //fixme
        )
      case r: PostUserByIdResponseNotFound.type =>
        Response(
          r.statusCode,
          Headers(Header.ContentType(MediaType.application.json).untyped),
          Body.fromCharSequence("""{"error": "Not Found"}""")
        )
    }

    def apply[T](value: T)(implicit ev: T => PostUserByIdResponse): PostUserByIdResponse = ev(value)

    implicit def OKEv(value: User): PostUserByIdResponse = OK(value)

    def OK(value: User): PostUserByIdResponse = PostUserByIdResponseOK(value)


    def NotFound: PostUserByIdResponse = PostUserByIdResponseNotFound
  }


}
