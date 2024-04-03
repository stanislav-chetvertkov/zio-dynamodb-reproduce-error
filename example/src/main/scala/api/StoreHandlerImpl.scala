package api

import service.ConfigurationService.User
import example.dao.Repository
import zio.{Task, ZLayer}

case class StoreHandlerImpl(repo: Repository) extends StoreHandler {

  override def getOrderById(respond: StoreResource.GetUserByIdResponse.type)
                           (id: Long): Task[StoreResource.GetUserByIdResponse] = {
    repo.read[User](id.toString)
      .mapBoth(
        e => StoreResource.ServiceError(e.getMessage),
        {
          case Some(user) => StoreResource.GetUserByIdResponse.OK(user)
          case None => StoreResource.GetUserByIdResponse.NotFound
        }
      )
  }


  override def postById(respond: StoreResource.PostUserByIdResponse.type)(id: Long, user: Protocol.CreateUser)
  : Task[StoreResource.PostUserByIdResponse] = {
    val userToSave = User(
      id = id.toString,
      region = user.region,
      code = user.code,
      parent = user.parent
    )
    repo.save(userToSave)
      .mapBoth(
        e => StoreResource.ServiceError(e.getMessage),
        _ => StoreResource.PostUserByIdResponse.OK(userToSave)
      )
  }
}

object StoreHandlerImpl {
  val live: ZLayer[Repository, Nothing, StoreHandler] = ZLayer.fromFunction(StoreHandlerImpl.apply _)
}
