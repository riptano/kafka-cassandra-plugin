/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package ly.stealth.kafka.plugin.cassandra.leader.election

import java.util.concurrent.{ScheduledFuture, ScheduledThreadPoolExecutor, TimeUnit}

import com.datastax.driver.core.{BoundStatement, ConsistencyLevel, Row, Session}
import ly.stealth.kafka.plugin.cassandra.listener.registry.CacheListenerRegistry
import ly.stealth.kafka.plugin.cassandra.util.{LogUtils, Config}
import org.apache.kafka.plugin.interface.{LeaderChangeListener, LeaderElection, ValueChangeListener}

import scala.collection.JavaConverters._
import scala.collection.mutable

class CassandraLeaderElection(session: Session,
                              config: Config,
                              resourceName: String = "kafka_controller") extends LeaderElection with LogUtils {

  this.logIdent = s"[LeaderElection]: "

  private lazy val getLeaderStmt = session
    .prepare(s"SELECT owner, sup_data FROM ${config.CassandraKeySpace}.leader_election WHERE resource = ?;")
    .setConsistencyLevel(ConsistencyLevel.SERIAL)

  private lazy val deleteLeaderStmt = session
    .prepare(s"DELETE FROM ${config.CassandraKeySpace}.leader_election where resource = ? IF owner = ?;")

  private lazy val tryAcquireLeadershipStmt = session
    .prepare(s"INSERT INTO ${config.CassandraKeySpace}.leader_election(resource, owner, sup_data) VALUES (?, ?, ?) IF NOT EXISTS;")

  private lazy val renewLeadershipStmt = session
    .prepare(s"UPDATE ${config.CassandraKeySpace}.leader_election set owner = ?, sup_data = ? where resource = ? IF owner = ?;")

  override def service: String = resourceName

  override def getLeader: Option[(String, String)] = {
    val boundStatement = new BoundStatement(getLeaderStmt)
    val results = session.execute(boundStatement.bind(resourceName))

    results.asScala.collectFirst { case r: Row => (r.getString("owner"), r.getString("sup_data")) }
  }

  private val ex = new ScheduledThreadPoolExecutor(config.LeaderElectionExecutorThreads)

  private val renewTaskLock = new Object()
  private var renewingTaskFuture: ScheduledFuture[_] = null

  private val cacheListenerRegistry = new CacheListenerRegistry(config, Some(ex))

  private def startRenewTask(candidate: String, supData: String): Unit = {
    renewTaskLock.synchronized {
      renewingTaskFuture = ex.scheduleAtFixedRate(new Runnable {
        override def run(): Unit = {
          trace(s"Renewing session for leader $candidate")

          val boundStatement = new BoundStatement(renewLeadershipStmt)
          session.execute(boundStatement.bind(candidate, supData, resourceName, candidate))

        }
      }, 0, config.LeaderElectionRenewTTLPeriod, TimeUnit.MILLISECONDS)
    }
  }

  val listeners = new mutable.ListBuffer[LeaderChangeListener]()

  private def tryAcquire(candidate: String, supData: String): Unit = {
    info(s"Trying to acquire leadership for $candidate")

    val boundStatement = new BoundStatement(tryAcquireLeadershipStmt)
    session.execute(boundStatement.bind(resourceName, candidate, supData))
  }

  private def cancelRenewTask(candidate: String) = {
    renewTaskLock.synchronized {
      if (renewingTaskFuture != null) {
        info(s"Renewing task is not empty - this candidate $candidate was a leader, cancelling renew task")
        renewingTaskFuture.cancel(true)
        renewingTaskFuture = null
      }
    }
  }

  private def setupLeaderWatchers(candidate: String, supData: String): Unit = {
    cacheListenerRegistry.addValueChangeListener(resourceName, getLeader.map(_._1), new ValueChangeListener {
      override def valueChanged(newValue: Option[String]): Unit = {
        info(s"New leader value - $newValue")

        newValue match {
          case Some(newLeader) =>
            if (newLeader == candidate) {
              info(s"Candidate $candidate acquired leadership, starting renewing task")
              startRenewTask(candidate, supData)
            } else {
              cancelRenewTask(candidate)
            }
          case None =>
            cancelRenewTask(candidate)
            tryAcquire(candidate, supData)
        }

        info(s"Calling on leader change listeners: ${listeners.size} total")
        listeners.synchronized {
          listeners.foreach {
            l => l.onLeaderChange(newValue)
          }
        }
      }
    })
  }

  override def nominate(candidate: String, supData: String) {
    setupLeaderWatchers(candidate, supData)
    tryAcquire(candidate, supData)
  }

  override def resign(leader: String): Unit = {
    val boundStatement = new BoundStatement(deleteLeaderStmt)
    session.execute(boundStatement.bind(resourceName, leader))
  }

  override def addListener(listener: LeaderChangeListener) = {
    listeners.synchronized {
      listeners += listener
    }
  }

  override def removeListener(listener: LeaderChangeListener) = {
    listeners.synchronized {
      listeners -= listener
    }
  }

  override def init(context: Any): Unit = {

  }

  override def close(): Unit = {

  }
}
