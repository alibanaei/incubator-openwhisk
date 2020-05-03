package org.apache.openwhisk.core.containerpool

import akka.actor.ActorSystem
import org.apache.openwhisk.common.Logging
import org.apache.openwhisk.core.database.CouchDbRestClient
import org.apache.openwhisk.core.entity._
import spray.json.{JsObject, JsString}

import scala.concurrent.duration.DurationInt
import scala.concurrent.Await
import scala.math.pow

class ResultProcess(namespace: EntityPath,
                    name: EntityName,
                    activationId: ActivationId,
                    limits:JsObject,
                    param:JsObject,
                    duration: Option[Long] = None,
                    logging:Logging
                   ){
  implicit val ec =  scala.concurrent.ExecutionContext.global

  val db = {
    implicit val sys = ActorSystem()
    implicit val log = logging.asInstanceOf[org.apache.openwhisk.common.Logging]
    new CouchDbRestClient("http", "172.17.245.8", 5984, "scc", "IUST9572", "action_time")(sys , log)
  }

  val generateID:String = {
    val memory = stringToInt(limits.fields("memory").toString)
    Math.abs(namespace.toString.hashCode + name.toString.hashCode + memory.hashCode).toString
  }

  val entryID:String = {
    Math.abs(param.hashCode).toString
  }

  def putdb {
    val doc = Await.result(db.getDoc(generateID), 20.seconds)
    doc match {
      case Right(response) =>
        var res = response.fields

        if (res.get(entryID).nonEmpty) {
          val oldData = res(entryID)
          res -= entryID

          val time = stringToInt(oldData.asJsObject.fields("time").toString)
          val count = stringToInt(oldData.asJsObject.fields("count").toString)
          val due = duration.getOrElse(0).toString.toLong
          val newTime = ((time * count).toLong + due) / (count + 1)
          val result = Map(
            "time" -> JsString(newTime.toString),
            "user" -> oldData.asJsObject.fields("user"),
            "name" -> oldData.asJsObject.fields("name"),
            "count" -> JsString((count + 1).toString)
          )
          res += (entryID -> JsObject(result))
        }
        else {
          val result = Map(
            "time" -> JsString(duration.getOrElse(0).toString),
            "user" -> JsString(namespace.toString),
            "name" -> JsString(name.name.toString),
            "count" -> JsString(1.toString)
          )
          res += (entryID -> JsObject(result))
        }

        res -= "totalTime"
        var count = 0
        var time = 0
        res.foreach(field => {
          if(field._1 != "_id" && field._1 != "_rev") {
            val c = stringToInt(field._2.asJsObject.fields("count").toString)
            count += c

            time += (stringToInt(field._2.asJsObject.fields("time").toString) * c)
          }
        })
        res += ("totalTime" -> JsString((time / count).toString))

        db.putDoc(generateID , JsObject(res))

      case _ =>

        val parameters = Map(
          "time" -> JsString(duration.getOrElse(0).toString),
          "user" -> JsString(namespace.toString),
          "name" -> JsString(name.name.toString),
          "count" -> JsString(1.toString)
        )

        val result = JsObject(Map(entryID -> JsObject(parameters), "totalTime" -> parameters("time")))
        db.putDoc(generateID, result)
    }
  }

  def getTime: Int ={
    var time = 0

    val doc = Await.result(db.getDoc(generateID) , 20.seconds)
    doc match {
      case Right(response) =>
        val data = response.fields
        time = if(data.get(entryID).nonEmpty){
                  val res = data(entryID)
                  stringToInt(res.asJsObject.fields("time").toString)
                }else{
                  stringToInt(data.get("totalTime").get.toString)
                }

      case _ =>

    }

    time
  }

  def stringToInt(str:String) = {
    val end = str.length-1
    val length = str.length-2
    var x = 0
    for(i <-1 until end){
      x += str(i).asDigit * pow(10 , length-i).toInt
    }
    x
  }

  def log = {
    logging.info(this , s"alii + namespace = ${namespace}")
    logging.info(this , s"alii + name = ${name}")
    logging.info(this , s"alii + activationId = ${activationId}")
    logging.info(this , s"alii + limits = ${limits.fields("memory")}")
    logging.info(this , s"alii + duration = ${duration}")
    logging.info(this , s"alii + param = ${param}")
  }

  /*
  def toJson:JsObject = {
    /*
    val ls = Map("namespace" -> namespace.toJson,
        "name" -> name.toJson ,
        "subject" -> JsString(subject.toString),
        "activationId" -> activationId.toJsObject,
        "start" ->JsString(start.toString),
        "end" -> JsString(end.toString),
        "response" -> response.toJsonObject,
        "version" -> JsString(version.toString),
        "annotations" -> annotations.toJsObject,
        "duration" -> JsString(duration.get.toString),
        "param" -> param)
    val entryID = param.toString.hashCode.toString
    val entry = Map(entryID -> JsString(Map("time" -> JsString(duration.get.toString) , "data" -> JsString(ls.toString)).toString))
    */

//    val time = JsString(duration.get.toString)

  }*/
}