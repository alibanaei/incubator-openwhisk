package org.apache.openwhisk.core.containerpool

import akka.actor.Actor
import org.apache.openwhisk.common.AkkaLogging
import org.apache.openwhisk.core.database.CouchDbRestClient
import spray.json.{JsObject, JsString}

import scala.collection.immutable
import scala.collection.mutable.ListBuffer

case class log(data: immutable.Map[String , immutable.Map[String , Any]])
case class throughPut(throughPut: immutable.Map[String , Any])
case class containerStateCount(cs: immutable.Map[String, Int])

case class r_free(s:Long, e:Long, free:Int)
case class resourceData(r:ListBuffer[r_free])

class DbSave extends Actor{
  implicit val logging = new AkkaLogging(context.system.log)
  implicit val ec =  scala.concurrent.ExecutionContext.global
  val db = (dbName: String) => {
    new CouchDbRestClient("http", "172.17.245.8", 5984, "scc", "IUST9572", dbName)(context.system , logging)
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
          logging.info(this, s"aliu log = ${result}")
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
      logging.info(this, s"aliu throughput = ${result}")

    case containerStateCount(data) =>
      val r = scala.util.Random
      val id = (
        (r.nextInt(10000)*r.nextInt(10000)) +
          (r.nextInt(1000)*r.nextInt(1000)) +
          (r.nextInt(100)*r.nextInt(100)) +
          (r.nextInt(10)*r.nextInt(10))
        ).toString

      var result = immutable.Map[String, JsString]()
      data.foreach{
        case r =>
          result += (r._1 -> JsString(r._2.toString))
      }
      db("container_state").putDoc(id, JsObject(result))
      logging.info(this, s"aliu db = ${result}")

    case resourceData(data) =>
      data.foreach{
        case r_free =>
          val result = Map(
            "start" -> JsString(r_free.s.toString),
            "end" -> JsString(r_free.e.toString),
            "free" -> JsString(r_free.free.toString),
          )

          val r = scala.util.Random
          val id = (
              System.currentTimeMillis() +
              (r.nextInt(10000)*r.nextInt(10000)) +
              (r.nextInt(1000)*r.nextInt(1000)) +
              (r.nextInt(100)*r.nextInt(100)) +
              (r.nextInt(10)*r.nextInt(10))
            ).toString
          db("resource").putDoc(id, JsObject(result))
          logging.info(this, s"aliu resource = ${result}")
      }
  }
}
