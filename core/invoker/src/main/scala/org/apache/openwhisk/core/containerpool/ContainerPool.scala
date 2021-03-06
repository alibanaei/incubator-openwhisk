/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.openwhisk.core.containerpool

import akka.actor.{Actor, ActorRef, ActorRefFactory, ActorSystem, Props}
import org.apache.openwhisk.common.{AkkaLogging, LoggingMarkers, TransactionId}
import org.apache.openwhisk.core.connector.MessageFeed
import org.apache.openwhisk.core.entity._
import org.apache.openwhisk.core.entity.size._

import scala.annotation.tailrec
import scala.collection.immutable
import scala.collection.mutable.ListBuffer
import scala.concurrent.duration._
import scala.util.Try

sealed trait WorkerState
case object Busy extends WorkerState
case object Free extends WorkerState

case class WorkerData(data: ContainerData, state: WorkerState)

/**
 * A pool managing containers to run actions on.
 *
 * This pool fulfills the other half of the ContainerProxy contract. Only
 * one job (either Start or Run) is sent to a child-actor at any given
 * time. The pool then waits for a response of that container, indicating
 * the container is done with the job. Only then will the pool send another
 * request to that container.
 *
 * Upon actor creation, the pool will start to prewarm containers according
 * to the provided prewarmConfig, iff set. Those containers will **not** be
 * part of the poolsize calculation, which is capped by the poolSize parameter.
 * Prewarm containers are only used, if they have matching arguments
 * (kind, memory) and there is space in the pool.
 *
 * @param childFactory method to create new container proxy actor
 * @param feed actor to request more work from
 * @param prewarmConfig optional settings for container prewarming
 * @param poolConfig config for the ContainerPool
 */
class ContainerPool(childFactory: ActorRefFactory => ActorRef,
                    feed: ActorRef,
                    prewarmConfig: List[PrewarmingConfig] = List.empty,
                    poolConfig: ContainerPoolConfig)
    extends Actor {
  import ContainerPool.memoryConsumptionOf

  implicit val logging = new AkkaLogging(context.system.log)

  var freePool = immutable.Map.empty[ActorRef, ContainerData]
  var busyPool = immutable.Map.empty[ActorRef, ContainerData]
  var prewarmedPool = immutable.Map.empty[ActorRef, ContainerData]
  // If all memory slots are occupied and if there is currently no container to be removed, than the actions will be
  // buffered here to keep order of computation.
  // Otherwise actions with small memory-limits could block actions with large memory limits.
  var runBuffer = immutable.Queue.empty[Run]
  val logMessageInterval = 10.seconds

  var mylog = immutable.Map.empty[String , immutable.Map[String , Any]]
  var throughPutLog = immutable.Map.empty[String , Any]
  var job = 0
  var containers_state = immutable.Map.empty[String , Int]
  var time_n:Long = 0

  var resource = ListBuffer[r_free]()

  val state = "FCFS"
//  val state = "ETAS"
//  val state = "BJF"
//  val state = "SJF"
//  val state = "MQS"

  case class act(r:Run, endTime:Long)
  case class act2(r:Run, arriveTime:Long, execTime:Int, endTime:Long)
  var runActions = ListBuffer[act]()
  var runActions2 = ListBuffer[act2]()

  var lock : Run = _
  var turn = ""
  var quantum = 0
  var maxQuantum = 0

  var actions = ListBuffer[Run]()
  var sActions = ListBuffer[Run]()
  var mActions = ListBuffer[Run]()
  var lActions = ListBuffer[Run]()

  var lock_index = 0
  val period_time = 1000

  prewarmConfig.foreach { config =>
    logging.info(this, s"pre-warming ${config.count} ${config.exec.kind} ${config.memoryLimit.toString}")(
      TransactionId.invokerWarmup)
    (1 to config.count).foreach { _ =>
      prewarmContainer(config.exec, config.memoryLimit)
    }
  }

  def logContainerStart(r: Run, containerState: String, activeActivations: Int, container: Option[Container]): Unit = {
    val namespaceName = r.msg.user.namespace.name
    val actionName = r.action.name.name
    val maxConcurrent = r.action.limits.concurrency.maxConcurrent
    val activationId = r.msg.activationId.toString

    r.msg.transid.mark(
      this,
      LoggingMarkers.INVOKER_CONTAINER_START(containerState),
      s"containerStart containerState: $containerState container: $container activations: $activeActivations of max $maxConcurrent action: $actionName namespace: $namespaceName activationId: $activationId",
      akka.event.Logging.InfoLevel)
  }

  def receive: Receive = {
    // A job to run on a container
    //
    // Run messages are received either via the feed or from child containers which cannot process
    // their requests and send them back to the pool for rescheduling (this may happen if "docker" operations
    // fail for example, or a container has aged and was destroying itself when a new request was assigned)
    case r: Run =>
      state match {
        case "FCFS" =>
          // Check if the message is resent from the buffer. Only the first message on the buffer can be resent.
          val isResentFromBuffer = runBuffer.nonEmpty && runBuffer.dequeueOption.exists(_._1.msg == r.msg)

          // Only process request, if there are no other requests waiting for free slots, or if the current request is the
          // next request to process
          // It is guaranteed, that only the first message on the buffer is resent.
          if (runBuffer.isEmpty || isResentFromBuffer) {

            if(!mylog.exists(_._1 == r.msg.activationId.toString)){
              mylog += (r.msg.activationId.toString -> Map(
                "action" -> r.action.name.name,
                "namespace" -> r.msg.user.namespace.name,
                "memory" -> r.action.limits.memory.megabytes.toInt,
                "queueSize" -> runBuffer.size,
                "start" -> System.currentTimeMillis,
                "executionTime" -> r.time,
                )
              )
            }

            val createdContainer =
            // Is there enough space on the invoker for this action to be executed.
              if (hasPoolSpaceFor(busyPool, r.action.limits.memory.megabytes.MB)) {
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

                val key = r.msg.activationId.toString
                var temp = mylog(key)
                temp += ("containerState" -> containerState)
                mylog = mylog.updated(key , temp)


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
                // Remove the action that get's executed now from the buffer and execute the next one afterwards.



                temp = mylog(key)
                temp += ("end" -> System.currentTimeMillis)
                mylog = mylog.updated(key , temp)


                if (isResentFromBuffer) {
                  // It is guaranteed that the currently executed messages is the head of the queue, if the message comes
                  // from the buffer
                  val (_, newBuffer) = runBuffer.dequeue
                  runBuffer = newBuffer
                  runBuffer.dequeueOption.foreach { case (run, _) => self ! run }

                  if(runBuffer.isEmpty){
                    val system = ActorSystem("dbsave")
                    val dbsave = system.actorOf(Props[DbSave] , "dbsave")
                    dbsave ! log(mylog)
                    mylog = mylog.empty

                    val new_resource = resource.clone()
                    dbsave ! resourceData(new_resource)
                    resource.clear()
                  }
                }
                else if(mylog.nonEmpty){
                  val system = ActorSystem("dbsave")
                  val dbsave = system.actorOf(Props[DbSave] , "dbsave")
                  dbsave ! log(mylog)
                  mylog = mylog.empty
                }

                actor ! r // forwards the run request to the container
                logContainerStart(r, containerState, newData.activeActivationCount, container)

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
                if (!isResentFromBuffer) {
                  // Add this request to the buffer, as it is not there yet.
                  runBuffer = runBuffer.enqueue(r)

                  val key = r.msg.activationId.toString
                  var temp = mylog(key)
                  temp = temp.updated("queueSize", runBuffer.size)
                  temp = temp.updated("start", System.currentTimeMillis)
                  mylog = mylog.updated(key , temp)

                  time_n = System.currentTimeMillis()
                }

                if((System.currentTimeMillis() - time_n).toInt >= period_time){
                  val free = (poolConfig.userMemory.toMB - memoryConsumptionOf(busyPool)).toInt
                  resource += r_free(time_n, System.currentTimeMillis(), free)
                  time_n = System.currentTimeMillis()
                }

                // As this request is the first one in the buffer, try again to execute it.
                self ! Run(r.action, r.msg, retryLogDeadline , r.time)
            }
          } else {
            // There are currently actions waiting to be executed before this action gets executed.
            // These waiting actions were not able to free up enough memory.
            runBuffer = runBuffer.enqueue(r)
            mylog += (r.msg.activationId.toString -> Map(
              "action" -> r.action.name.name,
              "namespace" -> r.msg.user.namespace.name,
              "memory" -> r.action.limits.memory.megabytes.toInt,
              "queueSize" -> runBuffer.size,
              "start" -> System.currentTimeMillis,
              "executionTime" -> r.time,
              )
            )
          }

        case "ETAS" =>
          // Check if the message is resent from the buffer. Only the first message on the buffer can be resent.
          var is_lock = runActions2.nonEmpty && lock.msg == r.msg

          var change_lock = false
          if(is_lock && lock_index == 0 && !hasPoolSpaceFor(busyPool, r.action.limits.memory.megabytes.MB)) {
            if(freePool.nonEmpty){
              runActions2.foreach {
                case act2 =>
                  if(!change_lock) {
                    val warm = ContainerPool.schedule(act2.r.action, act2.r.msg.user.namespace.name, freePool).getOrElse(false)
                    if (warm != false && (act2.arriveTime - 1000) <= runActions2(lock_index).arriveTime) {
                      change_lock = true
                      is_lock = false
                      lock = act2.r
                      lock_index = runActions2.indexOf(act2)
                      self ! act2.r
                    }
                  }
              }
            }
            if(!change_lock && memoryConsumptionOf(busyPool ++ freePool) < poolConfig.userMemory.toMB){
              runActions2.foreach {
                case act2 =>
                  if(!change_lock && (act2.arriveTime - 1000) <= runActions2(lock_index).arriveTime && hasPoolSpaceFor(busyPool ++ freePool, act2.r.action.limits.memory.megabytes.MB)) {
                    change_lock = true
                    is_lock = false
                    lock = act2.r
                    lock_index = runActions2.indexOf(act2)
                    self ! act2.r
                  }
              }
            }
          }


          // Only process request, if there are no other requests waiting for free slots, or if the current request is the
          // next request to process
          // It is guaranteed, that only the first message on the buffer is resent.
          if (runActions2.isEmpty || is_lock) {

            if(!mylog.exists(_._1 == r.msg.activationId.toString)){
              mylog += (r.msg.activationId.toString -> Map(
                "action" -> r.action.name.name,
                "namespace" -> r.msg.user.namespace.name,
                "memory" -> r.action.limits.memory.megabytes.toInt,
                "queueSize" -> runActions2.size,
                "start" -> System.currentTimeMillis,
                "executionTime" -> r.time,
                )
              )
            }

            val createdContainer =
            // Is there enough space on the invoker for this action to be executed.
              if (hasPoolSpaceFor(busyPool, r.action.limits.memory.megabytes.MB)) {

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

                val key = r.msg.activationId.toString
                var temp = mylog(key)
                temp += ("containerState" -> containerState)
                mylog = mylog.updated(key , temp)

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

                temp = mylog(key)
                temp += ("end" -> System.currentTimeMillis)
                mylog = mylog.updated(key , temp)

                actor ! r // forwards the run request to the container
                logContainerStart(r, containerState, newData.activeActivationCount, container)

                // Remove the action that get's executed now from the buffer and execute the next one afterwards.
                if (is_lock) {
                  // It is guaranteed that the currently executed messages is the head of the queue, if the message comes
                  // from the buffer
                  runActions2.remove(lock_index)

                  if(runActions2.nonEmpty){
                    runActions2 = runActions2.sortBy(_.endTime)
                    lock = runActions2.head.r
                    lock_index = 0
                    self ! lock
                  }
                  else{
                    val system = ActorSystem("dbsave")
                    val dbsave = system.actorOf(Props[DbSave] , "dbsave")
                    dbsave ! log(mylog)
                    mylog = mylog.empty

                    val new_resource = resource.clone()
                    dbsave ! resourceData(new_resource)
                    resource.clear()

                    lock_index = 0
                  }
                }else if(mylog.nonEmpty){
                  val system = ActorSystem("dbsave")
                  val dbsave = system.actorOf(Props[DbSave] , "dbsave")
                  dbsave ! log(mylog)
                  mylog = mylog.empty
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
                      s"waiting messages: ${runActions2.size}")(r.msg.transid)
                  Some(logMessageInterval.fromNow)
                } else {
                  r.retryLogDeadline
                }
                if (!is_lock) {
                  // Add this request to the buffer, as it is not there yet.
                  runActions2 += act2(r, System.currentTimeMillis(), r.time, System.currentTimeMillis() + r.time)
                  lock = r

                  val key = r.msg.activationId.toString
                  var temp = mylog(key)
                  temp = temp.updated("queueSize", runActions2.size)
                  temp = temp.updated("start", System.currentTimeMillis)
                  mylog = mylog.updated(key , temp)

                  time_n = System.currentTimeMillis()
                }
                // As this request is the first one in the buffer, try again to execute it.
                self ! Run(r.action, r.msg, retryLogDeadline , r.time)

                if(System.currentTimeMillis() - time_n >= period_time){
                  val free = (poolConfig.userMemory.toMB - memoryConsumptionOf(busyPool)).toInt
                  resource += r_free(time_n, System.currentTimeMillis(), free)
                  time_n = System.currentTimeMillis()
                }
            }
          } else if(!change_lock){
            // There are currently actions waiting to be executed before this action gets executed.
            // These waiting actions were not able to free up enough memory.
            runActions2 += act2(r, System.currentTimeMillis(), r.time, System.currentTimeMillis() + r.time)

            mylog += (r.msg.activationId.toString -> Map(
              "action" -> r.action.name.name,
              "namespace" -> r.msg.user.namespace.name,
              "memory" -> r.action.limits.memory.megabytes.toInt,
              "queueSize" -> runActions2.size,
              "start" -> System.currentTimeMillis,
              "executionTime" -> r.time,
              )
            )
          }

        case "BJF" =>
          // Check if the message is resent from the buffer. Only the first message on the buffer can be resent.
          val isResentFromBuffer = runActions.nonEmpty && runActions.head.r.msg == r.msg

          // Only process request, if there are no other requests waiting for free slots, or if the current request is the
          // next request to process
          // It is guaranteed, that only the first message on the buffer is resent.
          if (runActions.isEmpty || isResentFromBuffer) {

            if(!mylog.exists(_._1 == r.msg.activationId.toString)){
              mylog += (r.msg.activationId.toString -> Map(
                "action" -> r.action.name.name,
                "namespace" -> r.msg.user.namespace.name,
                "memory" -> r.action.limits.memory.megabytes.toInt,
                "queueSize" -> runActions.size,
                "start" -> System.currentTimeMillis,
                "executionTime" -> r.time,
               )
              )
            }

            val createdContainer =
            // Is there enough space on the invoker for this action to be executed.
              if (hasPoolSpaceFor(busyPool, r.action.limits.memory.megabytes.MB)) {

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

                val key = r.msg.activationId.toString
                var temp = mylog(key)
                temp += ("containerState" -> containerState)
                mylog = mylog.updated(key , temp)

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

                temp = mylog(key)
                temp += ("end" -> System.currentTimeMillis)
                mylog = mylog.updated(key , temp)

                actor ! r // forwards the run request to the container
                logContainerStart(r, containerState, newData.activeActivationCount, container)

                // Remove the action that get's executed now from the buffer and execute the next one afterwards.
                if (isResentFromBuffer) {
                  // It is guaranteed that the currently executed messages is the head of the queue, if the message comes
                  // from the buffer
                  runActions.remove(0)
                  if(runActions.nonEmpty){
                    runActions = runActions.sortBy(_.endTime)
                    self ! runActions.head.r
                  }
                  else{
                    val system = ActorSystem("dbsave")
                    val dbsave = system.actorOf(Props[DbSave] , "dbsave")
                    dbsave ! log(mylog)
                    mylog = mylog.empty

                    val new_resource = resource.clone()
                    dbsave ! resourceData(new_resource)
                    resource.clear()
                  }
                }else if(mylog.nonEmpty){
                  val system = ActorSystem("dbsave")
                  val dbsave = system.actorOf(Props[DbSave] , "dbsave")
                  dbsave ! log(mylog)
                  mylog = mylog.empty
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
                      s"waiting messages: ${runActions.size}")(r.msg.transid)
                  Some(logMessageInterval.fromNow)
                } else {
                  r.retryLogDeadline
                }
                if (!isResentFromBuffer) {
                  // Add this request to the buffer, as it is not there yet.
                  runActions += act(r, System.currentTimeMillis() + r.time)

                  val key = r.msg.activationId.toString
                  var temp = mylog(key)
                  temp = temp.updated("queueSize", runActions.size)
                  temp = temp.updated("start", System.currentTimeMillis)
                  mylog = mylog.updated(key , temp)

                  time_n = System.currentTimeMillis()
                }

                if(System.currentTimeMillis() - time_n >= period_time){
                  val free = (poolConfig.userMemory.toMB - memoryConsumptionOf(busyPool)).toInt
                  resource += r_free(time_n, System.currentTimeMillis(), free)
                  time_n = System.currentTimeMillis()
                }

                // As this request is the first one in the buffer, try again to execute it.
                self ! Run(r.action, r.msg, retryLogDeadline , r.time)
            }
          } else {
            // There are currently actions waiting to be executed before this action gets executed.
            // These waiting actions were not able to free up enough memory.
            runActions += act(r, System.currentTimeMillis() + r.time)

            mylog += (r.msg.activationId.toString -> Map(
              "action" -> r.action.name.name,
              "namespace" -> r.msg.user.namespace.name,
              "memory" -> r.action.limits.memory.megabytes.toInt,
              "queueSize" -> runActions.size,
              "start" -> System.currentTimeMillis,
              "executionTime" -> r.time,
              )
            )
          }

        case "SJF" =>
          // Check if the message is resent from the buffer. Only the first message on the buffer can be resent.
          val isResentFromBuffer = runActions.nonEmpty && runActions.head.r.msg == r.msg

          // Only process request, if there are no other requests waiting for free slots, or if the current request is the
          // next request to process
          // It is guaranteed, that only the first message on the buffer is resent.
          if (runActions.isEmpty || isResentFromBuffer) {

            if(!mylog.exists(_._1 == r.msg.activationId.toString)){
              mylog += (r.msg.activationId.toString -> Map(
                "action" -> r.action.name.name,
                "namespace" -> r.msg.user.namespace.name,
                "memory" -> r.action.limits.memory.megabytes.toInt,
                "queueSize" -> runActions.size,
                "start" -> System.currentTimeMillis,
                "executionTime" -> r.time,
                )
              )
            }

            val createdContainer =
            // Is there enough space on the invoker for this action to be executed.
              if (hasPoolSpaceFor(busyPool, r.action.limits.memory.megabytes.MB)) {

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

                val key = r.msg.activationId.toString
                var temp = mylog(key)
                temp += ("containerState" -> containerState)
                mylog = mylog.updated(key , temp)

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


                temp = mylog(key)
                temp += ("end" -> System.currentTimeMillis)
                mylog = mylog.updated(key , temp)


                actor ! r // forwards the run request to the container
                logContainerStart(r, containerState, newData.activeActivationCount, container)

                // Remove the action that get's executed now from the buffer and execute the next one afterwards.
                if (isResentFromBuffer) {
                  // It is guaranteed that the currently executed messages is the head of the queue, if the message comes
                  // from the buffer
                  runActions.remove(0)
                  if(runActions.nonEmpty){
                    runActions = runActions.sortBy(_.endTime)
                    self ! runActions.head.r
                  }
                  else{
                    val system = ActorSystem("dbsave")
                    val dbsave = system.actorOf(Props[DbSave] , "dbsave")
                    dbsave ! log(mylog)
                    mylog = mylog.empty

                    val new_resource = resource.clone()
                    dbsave ! resourceData(new_resource)
                    resource.clear()
                  }
                }else if(mylog.nonEmpty){
                  val system = ActorSystem("dbsave")
                  val dbsave = system.actorOf(Props[DbSave] , "dbsave")
                  dbsave ! log(mylog)
                  mylog = mylog.empty
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
                      s"waiting messages: ${runActions.size}")(r.msg.transid)
                  Some(logMessageInterval.fromNow)
                } else {
                  r.retryLogDeadline
                }
                if (!isResentFromBuffer) {
                  // Add this request to the buffer, as it is not there yet.
                  runActions += act(r, r.time)

                  val key = r.msg.activationId.toString
                  var temp = mylog(key)
                  temp = temp.updated("queueSize", runActions.size)
                  temp = temp.updated("start", System.currentTimeMillis)
                  mylog = mylog.updated(key , temp)

                  time_n = System.currentTimeMillis()
                }

                if(System.currentTimeMillis() - time_n >= period_time){
                  val free = (poolConfig.userMemory.toMB - memoryConsumptionOf(busyPool)).toInt
                  resource += r_free(time_n, System.currentTimeMillis(), free)
                  time_n = System.currentTimeMillis()
                }

                // As this request is the first one in the buffer, try again to execute it.
                self ! Run(r.action, r.msg, retryLogDeadline , r.time)
            }
          } else {
            // There are currently actions waiting to be executed before this action gets executed.
            // These waiting actions were not able to free up enough memory.
            runActions += act(r, r.time)
            mylog += (r.msg.activationId.toString -> Map(
              "action" -> r.action.name.name,
              "namespace" -> r.msg.user.namespace.name,
              "memory" -> r.action.limits.memory.megabytes.toInt,
              "queueSize" -> runActions.size,
              "start" -> System.currentTimeMillis,
              "executionTime" -> r.time,
              )
            )
          }

        case  "MQS" =>
          // Check if the message is resent from the buffer. Only the first message on the buffer can be resent.
          val empty = actions.isEmpty && sActions.isEmpty && mActions.isEmpty && lActions.isEmpty
          val isLocked = !empty && lock.msg == r.msg

          // Only process request, if there are no other requests waiting for free slots, or if the current request is the
          // next request to process
          // It is guaranteed, that only the first message on the buffer is resent.
          if (empty || isLocked) {

            if(!mylog.exists(_._1 == r.msg.activationId.toString)){
              mylog += (r.msg.activationId.toString -> Map(
                "action" -> r.action.name.name,
                "namespace" -> r.msg.user.namespace.name,
                "memory" -> r.action.limits.memory.megabytes.toInt,
                "queueSize" -> (actions.size + sActions.size + mActions.size + lActions.size),
                "start" -> System.currentTimeMillis,
                "executionTime" -> r.time,
                )
              )
            }

            val createdContainer =
            // Is there enough space on the invoker for this action to be executed.
              if (hasPoolSpaceFor(busyPool, r.action.limits.memory.megabytes.MB)) {

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

                val key = r.msg.activationId.toString
                var temp = mylog(key)
                temp += ("containerState" -> containerState)
                mylog = mylog.updated(key , temp)

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

                temp = mylog(key)
                temp += ("end" -> System.currentTimeMillis)
                mylog = mylog.updated(key , temp)


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

                        val new_resource = resource.clone()
                        dbsave ! resourceData(new_resource)
                        resource.clear()
                    }
                  }
                }else if(mylog.nonEmpty){
                  val system = ActorSystem("dbsave")
                  val dbsave = system.actorOf(Props[DbSave] , "dbsave")
                  dbsave ! log(mylog)
                  mylog = mylog.empty
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

                  val key = r.msg.activationId.toString
                  var temp = mylog(key)
                  temp = temp.updated("queueSize", (actions.size + sActions.size + mActions.size + lActions.size))
                  temp = temp.updated("start", System.currentTimeMillis)
                  mylog = mylog.updated(key , temp)

                  time_n = System.currentTimeMillis()
                }

                if(System.currentTimeMillis() - time_n >= period_time){
                  val free = (poolConfig.userMemory.toMB - memoryConsumptionOf(busyPool)).toInt
                  resource += r_free(time_n, System.currentTimeMillis(), free)
                  time_n = System.currentTimeMillis()
                }

                // As this request is the first one in the buffer, try again to execute it.
                self ! Run(r.action, r.msg, retryLogDeadline , r.time)
            }
          }
          else {
            // There are currently actions waiting to be executed before this action gets executed.
            // These waiting actions were not able to free up enough memory.
            actions += r

            mylog += (r.msg.activationId.toString -> Map(
              "action" -> r.action.name.name,
              "namespace" -> r.msg.user.namespace.name,
              "memory" -> r.action.limits.memory.megabytes.toInt,
              "queueSize" -> (actions.size + sActions.size + mActions.size + lActions.size),
              "start" -> System.currentTimeMillis,
              "executionTime" -> r.time,
              )
            )
          }
      }

    // Container is free to take more work
    case NeedWork(warmData: WarmedData) =>
      feed ! MessageFeed.Processed
      val oldData = freePool.get(sender()).getOrElse(busyPool(sender()))
      val newData = warmData.copy(activeActivationCount = oldData.activeActivationCount - 1)
      if (newData.activeActivationCount < 0) {
        logging.error(this, s"invalid activation count after warming < 1 ${newData}")
      }
      if (newData.hasCapacity()) {
        //remove from busy pool (may already not be there), put back into free pool (to update activation counts)
        freePool = freePool + (sender() -> newData)
        if (busyPool.contains(sender())) {
          busyPool = busyPool - sender()
          if (newData.action.limits.concurrency.maxConcurrent > 1) {
            logging.info(
              this,
              s"concurrent container ${newData.container} is no longer busy with ${newData.activeActivationCount} activations")
          }
        }
      } else {
        busyPool = busyPool + (sender() -> newData)
        freePool = freePool - sender()
      }

    // Container is prewarmed and ready to take work
    case NeedWork(data: PreWarmedData) =>
      prewarmedPool = prewarmedPool + (sender() -> data)

    // Container got removed
    case ContainerRemoved =>
      // if container was in free pool, it may have been processing (but under capacity),
      // so there is capacity to accept another job request
      freePool.get(sender()).foreach { f =>
        freePool = freePool - sender()
        if (f.activeActivationCount > 0) {
          feed ! MessageFeed.Processed
        }
      }
      // container was busy (busy indicates at full capacity), so there is capacity to accept another job request
      busyPool.get(sender()).foreach { _ =>
        busyPool = busyPool - sender()
        feed ! MessageFeed.Processed
      }

    // This message is received for one of these reasons:
    // 1. Container errored while resuming a warm container, could not process the job, and sent the job back
    // 2. The container aged, is destroying itself, and was assigned a job which it had to send back
    // 3. The container aged and is destroying itself
    // Update the free/busy lists but no message is sent to the feed since there is no change in capacity yet
    case RescheduleJob =>
      freePool = freePool - sender()
      busyPool = busyPool - sender()
  }

  /** Creates a new container and updates state accordingly. */
  def createContainer(memoryLimit: ByteSize): (ActorRef, ContainerData) = {
    val ref = childFactory(context)
    val data = MemoryData(memoryLimit)
    freePool = freePool + (ref -> data)
    ref -> data
  }

  /** Creates a new prewarmed container */
  def prewarmContainer(exec: CodeExec[_], memoryLimit: ByteSize): Unit =
    childFactory(context) ! Start(exec, memoryLimit)

  /**
   * Takes a prewarm container out of the prewarmed pool
   * iff a container with a matching kind and memory is found.
   *
   * @param action the action that holds the kind and the required memory.
   * @return the container iff found
   */
  def takePrewarmContainer(action: ExecutableWhiskAction): Option[(ActorRef, ContainerData)] = {
    val kind = action.exec.kind
    val memory = action.limits.memory.megabytes.MB
    prewarmedPool
      .find {
        case (_, PreWarmedData(_, `kind`, `memory`, _)) => true
        case _                                          => false
      }
      .map {
        case (ref, data) =>
          // Move the container to the usual pool
          freePool = freePool + (ref -> data)
          prewarmedPool = prewarmedPool - ref
          // Create a new prewarm container
          // NOTE: prewarming ignores the action code in exec, but this is dangerous as the field is accessible to the
          // factory
          prewarmContainer(action.exec, memory)
          (ref, data)
      }
  }

  /** Removes a container and updates state accordingly. */
  def removeContainer(toDelete: ActorRef) = {
    toDelete ! Remove
    freePool = freePool - toDelete
    busyPool = busyPool - toDelete
  }

  /**
   * Calculate if there is enough free memory within a given pool.
   *
   * @param pool The pool, that has to be checked, if there is enough free memory.
   * @param memory The amount of memory to check.
   * @return true, if there is enough space for the given amount of memory.
   */
  def hasPoolSpaceFor[A](pool: Map[A, ContainerData], memory: ByteSize): Boolean = {
    memoryConsumptionOf(pool) + memory.toMB <= poolConfig.userMemory.toMB
  }
}

object ContainerPool {

  /**
   * Calculate the memory of a given pool.
   *
   * @param pool The pool with the containers.
   * @return The memory consumption of all containers in the pool in Megabytes.
   */
  protected[containerpool] def memoryConsumptionOf[A](pool: Map[A, ContainerData]): Long = {
    pool.map(_._2.memoryLimit.toMB).sum
  }

  /**
   * Finds the best container for a given job to run on.
   *
   * Selects an arbitrary warm container from the passed pool of idle containers
   * that matches the action and the invocation namespace. The implementation uses
   * matching such that structural equality of action and the invocation namespace
   * is required.
   * Returns None iff no matching container is in the idle pool.
   * Does not consider pre-warmed containers.
   *
   * @param action the action to run
   * @param invocationNamespace the namespace, that wants to run the action
   * @param idles a map of idle containers, awaiting work
   * @return a container if one found
   */
  protected[containerpool] def schedule[A](action: ExecutableWhiskAction,
                                           invocationNamespace: EntityName,
                                           idles: Map[A, ContainerData]): Option[(A, ContainerData)] = {
    idles
      .find {
        case (_, c @ WarmedData(_, `invocationNamespace`, `action`, _, _)) if c.hasCapacity() => true
        case _                                                                                => false
      }
      .orElse {
        idles.find {
          case (_, c @ WarmingData(_, `invocationNamespace`, `action`, _, _)) if c.hasCapacity() => true
          case _                                                                                 => false
        }
      }
      .orElse {
        idles.find {
          case (_, c @ WarmingColdData(`invocationNamespace`, `action`, _, _)) if c.hasCapacity() => true
          case _                                                                                  => false
        }
      }
  }

  /**
   * Finds the oldest previously used container to remove to make space for the job passed to run.
   * Depending on the space that has to be allocated, several containers might be removed.
   *
   * NOTE: This method is never called to remove an action that is in the pool already,
   * since this would be picked up earlier in the scheduler and the container reused.
   *
   * @param pool a map of all free containers in the pool
   * @param memory the amount of memory that has to be freed up
   * @return a list of containers to be removed iff found
   */
  @tailrec
  protected[containerpool] def remove[A](pool: Map[A, ContainerData],
                                         memory: ByteSize,
                                         toRemove: List[A] = List.empty): List[A] = {
    // Try to find a Free container that does NOT have any active activations AND is initialized with any OTHER action
    val freeContainers = pool.collect {
      // Only warm containers will be removed. Prewarmed containers will stay always.
      case (ref, w: WarmedData) if w.activeActivationCount == 0 =>
        ref -> w
    }

    if (memory > 0.B && freeContainers.nonEmpty && memoryConsumptionOf(freeContainers) >= memory.toMB) {
      // Remove the oldest container if:
      // - there is more memory required
      // - there are still containers that can be removed
      // - there are enough free containers that can be removed
      val (ref, data) = freeContainers.minBy(_._2.lastUsed)
      // Catch exception if remaining memory will be negative
      val remainingMemory = Try(memory - data.memoryLimit).getOrElse(0.B)
      remove(freeContainers - ref, remainingMemory, toRemove ++ List(ref))
    } else {
      // If this is the first call: All containers are in use currently, or there is more memory needed than
      // containers can be removed.
      // Or, if this is one of the recursions: Enough containers are found to get the memory, that is
      // necessary. -> Abort recursion
      toRemove
    }
  }

  def props(factory: ActorRefFactory => ActorRef,
            poolConfig: ContainerPoolConfig,
            feed: ActorRef,
            prewarmConfig: List[PrewarmingConfig] = List.empty) =
    Props(new ContainerPool(factory, feed, prewarmConfig, poolConfig))
}

/** Contains settings needed to perform container prewarming. */
case class PrewarmingConfig(count: Int, exec: CodeExec[_], memoryLimit: ByteSize)
