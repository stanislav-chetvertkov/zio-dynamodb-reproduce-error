package api.generic

import api.Protocol.CreateUser
import api.StoreHandler
import api.generic.GenericHandlerImpl.Processors
import configuration.ConfigSchemaCodec
import configuration.dao.Repository
import service.ConfigurationService.User
import zio.{Chunk, RuntimeFlags, ZIO, ZLayer}
import zio.http.{Body, Handler, Header, Headers, MediaType, Method, Request, Response, Routes, Status, int, string}
import zio.json.{DeriveJsonDecoder, JsonDecoder}
import zio.schema.codec.JsonCodec

trait GenericHandler {

  def listResource(resourceName: String): ZIO[Any, GenericResource.Error, String]

}

object GenericHandlerImpl {
  case class Processors(processors: Map[String, ConfigSchemaCodec[_]])
  
  val live: ZLayer[Repository & Processors, Nothing, GenericHandler] = ZLayer.fromZIO(for {
    processors <- ZIO.service[Processors]
    repository <- ZIO.service[Repository]
  } yield GenericHandlerImpl(processors, repository): GenericHandler)
}

class GenericHandlerImpl(processors: Processors, repository: Repository) extends GenericHandler {

  override def listResource(resourceName: String)
  : ZIO[Any, GenericResource.Error, String] = {
    processors.processors.get(resourceName) match {
      case Some(processor) =>
        processor match {
          case typedProcessor: ConfigSchemaCodec[t] =>
            given ConfigSchemaCodec[t] = typedProcessor

            val x: ZIO[Any, Throwable, Chunk[t]] = repository.listAll[t]

            x.map(_.map(JsonCodec.jsonEncoder(typedProcessor.derivedFrom).encodeJson(_)).toString())
              .mapError(e => GenericResource.ServiceError(e.getMessage))
          case _ => throw GenericResource.ServiceError("Invalid processor type")
        }
      case None => throw GenericResource.ServiceError("Resource not found")
    }
  }
}


object GenericResource {

  sealed trait Error extends Exception

  private case class DecodingError(message: String) extends Error {
    override def getMessage: String = message
  }

  case class ServiceError(message: String) extends Error

  def routes(impl: GenericHandler): Routes[Any, Response] = Routes(
    Method.GET / "resources" / string("resource") -> {
      Handler.fromFunctionZIO { (in: (String, Request)) =>
        impl.listResource(in._1)
          .map(r => Response.text(r).status(Status.Ok))
      }.mapError(e => Response.text("Error: " + e.getMessage).status(Status.InternalServerError))
    }
  )

}
