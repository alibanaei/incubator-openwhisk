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
  }
}










/*
* case  "MQS" =>
          // Check if the message is resent from the buffer. Only the first message on the buffer can be resent.
          val empty = actions.isEmpty && sActions.isEmpty && mActions.isEmpty && lActions.isEmpty
          val isLocked = !empty && lock.msg == r.msg


          // Only process request, if there are no other requests waiting for free slots, or if the current request is the
          // next request to process
          // It is guaranteed, that only the first message on the buffer is resent.
          if (empty || isLocked) {
            val createdContainer =
            // Is there enough space on the invoker for this action to be executed.
              if (hasPoolSpaceFor(busyPool, r.action.limits.memory.megabytes.MB)) {

                if(mylog.exists(_._1 == r.msg.activationId.toString)){
                  val key = r.msg.activationId.toString
                  var temp = mylog(key)
                  temp += ("end" -> System.currentTimeMillis)
                  mylog = mylog.updated(key , temp)
                }
                if(throughPutLog.exists(_._1 == "startTime") && !throughPutLog.exists(_._1 == "endTime")){
                  job += 1
                }

                // Schedule a job to a warm container
                ContainerPool
                  .schedule(r.action, r.msg.user.namespace.name, freePool)
                  .map(container => (container, container._2.initingState)) //warmed, warming, and warmingCold always know their state
                  .orElse(
                  // There was no warm/warming/warmingCold container. Try to take a prewarm container or a cold container.

                  // Is there enough space to create a new container or do other containers have to be removed?
                  if (hasPoolSpaceFor(busyPool ++ freePool, r.action.limits.memory.megabytes.MB)) {
                    takePrewarmContainer(r.action)
                      .map(container => (container, "prewarmed"))
                      .orElse(Some(createContainer(r.action.limits.memory.megabytes.MB), "cold"))
                  } else None)
                  .orElse(
                    // Remove a container and create a new one for the given job
                    ContainerPool
                      // Only free up the amount, that is really needed to free up
                      .remove(freePool, Math.min(r.action.limits.memory.megabytes, memoryConsumptionOf(freePool)).MB)
                      .map(removeContainer)
                      // If the list had at least one entry, enough containers were removed to start the new container. After
                      // removing the containers, we are not interested anymore in the containers that have been removed.
                      .headOption
                      .map(_ =>
                        takePrewarmContainer(r.action)
                          .map(container => (container, "recreatedPrewarm"))
                          .getOrElse(createContainer(r.action.limits.memory.megabytes.MB), "recreated")))

              } else None

            createdContainer match {
              case Some(((actor, data), containerState)) =>
                //increment active count before storing in pool map
                val newData = data.nextRun(r)
                val container = newData.getContainer

                if (newData.activeActivationCount < 1) {
                  logging.error(this, s"invalid activation count < 1 ${newData}")
                }

                //only move to busyPool if max reached
                if (!newData.hasCapacity()) {
                  if (r.action.limits.concurrency.maxConcurrent > 1) {
                    logging.info(
                      this,
                      s"container ${container} is now busy with ${newData.activeActivationCount} activations")
                  }
                  busyPool = busyPool + (actor -> newData)
                  freePool = freePool - actor
                } else {
                  //update freePool to track counts
                  freePool = freePool + (actor -> newData)
                }

                actor ! r // forwards the run request to the container
                logContainerStart(r, containerState, newData.activeActivationCount, container)

                // Remove the action that get's executed now from the buffer and execute the next one afterwards.
                if (isLocked) {
                  // It is guaranteed that the currently executed messages is the head of the queue, if the message comes
                  // from the buffer
                  quantum -= 1
                  turn match {
                    case "main" =>
                      actions.remove(0)
                      if(quantum > 0 && actions.nonEmpty){
                        lock = actions.head
                        self ! lock
                      }else{
                        quantum = 0
                      }

                    case "small" =>
                      sActions.remove(0)
                      if(quantum > 0 && sActions.nonEmpty){
                        lock = sActions.head
                        self ! lock
                      }else{
                        quantum = 0
                      }

                    case "medium" =>
                      mActions.remove(0)
                      if(quantum > 0 && mActions.nonEmpty){
                        lock = mActions.head
                        self ! lock
                      }else{
                        quantum = 0
                      }

                    case "large" =>
                      lActions.remove(0)
                      if(quantum > 0 && lActions.nonEmpty){
                        lock = lActions.head
                        self ! lock
                      }else{
                        quantum = 0
                      }
                  }

                  if(quantum <= 0){
                    var choose = ""
                    if(turn == "small"){
                      if(mActions.nonEmpty) {
                        choose = "medium"
                      }
                      else if(lActions.nonEmpty){
                        choose = "large"
                      }
                      else if(sActions.nonEmpty){
                        choose = "small"
                      }
                      else if(actions.nonEmpty){
                        choose = "main"
                      }
                    }
                    if(turn == "medium"){
                      if(lActions.nonEmpty) {
                        choose = "large"
                      }
                      else if(sActions.nonEmpty){
                        choose = "small"
                      }
                      else if(mActions.nonEmpty){
                        choose = "medium"
                      }
                      else if(actions.nonEmpty){
                        choose = "main"
                      }
                    }
                    if(turn == "large"){
                      if(sActions.nonEmpty) {
                        choose = "small"
                      }
                      else if(mActions.nonEmpty){
                        choose = "medium"
                      }
                      else if(lActions.nonEmpty){
                        choose = "large"
                      }
                      else if(actions.nonEmpty){
                        choose = "main"
                      }
                    }
                    if(turn == "main" && actions.nonEmpty){
                      choose = "main"
                    }

                    choose match{
                      case "main" =>
                        if(actions.size < 10){
                          actions = actions.sortBy(_.time)
                          quantum = actions.size
                          turn = "main"
                          lock = actions.head
                          self ! lock
                        }
                        else{
                          val size = actions.size
                          actions = actions.sortBy(_.time)
                          val forty = (size * 0.4).toInt + 1

                          for(i <- 0 until size){
                            if(i < forty){
                              sActions += actions.head
                            }
                            else if(i < 2*forty){
                              mActions += actions.head
                            }
                            else{
                              lActions += actions.head
                            }
                            actions.remove(0)
                          }
                          sActions = sActions.sortBy(_.time)
                          mActions = mActions.sortBy(_.time)
                          lActions = lActions.sortBy(_.time)

                          maxQuantum = size / 5
                          turn = "small"
                          quantum = maxQuantum
                          lock = sActions.head
                          self ! lock
                        }

                      case "small" =>
                        turn = "small"
                        quantum = maxQuantum
                        lock = sActions.head
                        self ! lock

                      case "medium" =>
                        turn = "medium"
                        quantum = maxQuantum
                        lock = mActions.head
                        self ! lock

                      case "large" =>
                        turn = "large"
                        quantum = maxQuantum / 2
                        lock = lActions.head
                        self ! lock

                      case _ =>
                        val system = ActorSystem("dbsave")
                        val dbsave = system.actorOf(Props[DbSave] , "dbsave")
                        dbsave ! log(mylog)
                        mylog = mylog.empty

                        throughPutLog += ("endTime" -> System.currentTimeMillis() , "job" -> job)
                        dbsave ! throughPut(throughPutLog)
                        throughPutLog = throughPutLog.empty
                        job = 0
                    }
                  }
                }

              case None =>
                // this can also happen if createContainer fails to start a new container, or
                // if a job is rescheduled but the container it was allocated to has not yet destroyed itself
                // (and a new container would over commit the pool)
                val isErrorLogged = r.retryLogDeadline.map(_.isOverdue).getOrElse(true)
                val retryLogDeadline = if (isErrorLogged) {
                  logging.error(
                    this,
                    s"Rescheduling Run message, too many message in the pool, " +
                      s"freePoolSize: ${freePool.size} containers and ${memoryConsumptionOf(freePool)} MB, " +
                      s"busyPoolSize: ${busyPool.size} containers and ${memoryConsumptionOf(busyPool)} MB, " +
                      s"maxContainersMemory ${poolConfig.userMemory.toMB} MB, " +
                      s"userNamespace: ${r.msg.user.namespace.name}, action: ${r.action}, " +
                      s"needed memory: ${r.action.limits.memory.megabytes} MB, " +
                      s"waiting messages: ${runBuffer.size}")(r.msg.transid)
                  Some(logMessageInterval.fromNow)
                } else {
                  r.retryLogDeadline
                }
                if (!isLocked) {
                  // Add this request to the buffer, as it is not there yet.
                  actions += r
                  lock = r
                  turn = "main"

                  mylog += (r.msg.activationId.toString -> Map("namespace" -> r.msg.user.namespace.name , "queueSize" -> (actions.size + sActions.size + mActions.size + lActions.size) , "start" -> System.currentTimeMillis))
                  throughPutLog += ("startTime" -> System.currentTimeMillis)
                }

                // As this request is the first one in the buffer, try again to execute it.
                self ! Run(r.action, r.msg, retryLogDeadline , r.time)
            }
          }
          else {
            // There are currently actions waiting to be executed before this action gets executed.
            // These waiting actions were not able to free up enough memory.
            actions += r
            mylog += (r.msg.activationId.toString -> Map("namespace" -> r.msg.user.namespace.name , "queueSize" -> (actions.size + sActions.size + mActions.size + lActions.size) , "start" -> System.currentTimeMillis))
          }
* */