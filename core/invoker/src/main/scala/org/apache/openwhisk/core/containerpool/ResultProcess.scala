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
    new CouchDbRestClient("http", "172.17.245.7", 5984, "scc", "IUST9572", "action_time")(sys , log)
  }

  val generateID:String = {
    val memory = stringToInt(limits.fields("memory").toString)
    Math.abs(namespace.toString.hashCode + name.toString.hashCode + memory.hashCode).toString
  }

  val entryID:String = {
    Math.abs(param.hashCode).toString
  }

  def putdb {
    val doc = db.getDoc(generateID)

    doc.map{
      case Right(response) =>
        var newRes = response
        if(response.getFields(entryID).nonEmpty){
          val time = stringToInt(response.getFields(entryID)(0).toString)
          val newTime = (duration.get.toInt + time) / 2
          newRes = JsObject(newRes.fields + (entryID -> JsString(newTime.toString)))
        }else{
          newRes = JsObject(newRes.fields + (entryID -> JsString(duration.get.toString)))
        }

        if (response.getFields("totalTime").nonEmpty) {
          var avg = 0
          var total = 0

          newRes.fields.foreach(field => {
            if(field._1 != "_id" && field._1 != "_rev"){
              avg += stringToInt(field._2.toString)
              total += 1
            }
          })
          val newTime = avg/total
          newRes = JsObject(newRes.fields + ("totalTime" -> JsString(newTime.toString)))

        } else {
          newRes = JsObject(newRes.fields + ("totalTime" -> JsString(duration.get.toString)))
        }

        db.putDoc(generateID , newRes)

      case _ =>
        val time = JsString(duration.get.toInt.toString)
        val entry = Map(entryID -> time , "totalTime" -> time)
        db.putDoc(generateID , JsObject(entry))
    }(ec)

  }

  def getTime: Int ={
    var time = 0
    val doc = Await.result(db.getDoc(generateID) , 20.seconds)
    doc match {
      case Right(response) =>
        time = stringToInt(response.fields("totalTime").toString)
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