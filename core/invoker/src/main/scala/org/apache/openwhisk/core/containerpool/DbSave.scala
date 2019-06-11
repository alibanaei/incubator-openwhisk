package org.apache.openwhisk.core.containerpool

import akka.actor.Actor
import org.apache.openwhisk.common.AkkaLogging
import org.apache.openwhisk.core.database.CouchDbRestClient
import spray.json.{JsObject, JsString}

import scala.collection.immutable

case class log(data: immutable.Map[String , immutable.Map[String , Any]])
case class throughPut(throughPut: immutable.Map[String , Any])

class DbSave extends Actor{
  implicit val logging = new AkkaLogging(context.system.log)
  implicit val ec =  scala.concurrent.ExecutionContext.global
  val db = (dbName: String) => {
    new CouchDbRestClient("http", "172.17.245.7", 5984, "scc", "IUST9572", dbName)(context.system , logging)
  }

  def receive: Receive = {
    case log(data) =>
      data.foreach(mylog => {
        if(mylog._2.size == 4) {
          val result = Map(
            "namespace" -> JsString(mylog._2("namespace").toString),
            "queueSize" -> JsString(mylog._2("queueSize").toString),
            "start" -> JsString(mylog._2("start").toString),
            "end" -> JsString(mylog._2("end").toString)
          )
          db("logging").putDoc(mylog._1, JsObject(result))
        }
      })

    case throughPut(data) =>
      val result = Map(
        "end_time" -> JsString(data("endTime").toString),
        "start_Time" -> JsString(data("startTime").toString),
        "job_count" -> JsString(data("job").toString)
      )
      val r = scala.util.Random
      val id = (
          (r.nextInt(10000)*r.nextInt(10000)) +
          (r.nextInt(1000)*r.nextInt(1000)) +
          (r.nextInt(100)*r.nextInt(100)) +
          (r.nextInt(10)*r.nextInt(10))
        ).toString
      db("throughput").putDoc(id, JsObject(result))
  }
}