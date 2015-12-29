/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package ly.stealth.kafka.plugin.cassandra

import java.io.File

import com.datastax.driver.core.{Session, Cluster}
import ly.stealth.kafka.plugin.cassandra.group.membership.CassandraGroupMembership
import ly.stealth.kafka.plugin.cassandra.leader.election.CassandraLeaderElection
import ly.stealth.kafka.plugin.cassandra.listener.registry.CassandraListenerRegistry
import ly.stealth.kafka.plugin.cassandra.util.Config
import org.apache.kafka.plugin.interface.{GroupMembership, PluginLocator, ListenerRegistry, LeaderElection}

class CassandraPluginLocator extends PluginLocator {

  private var session: Session = null
  private var config: Config = null

  private var leaderElection: LeaderElection = null
  private var listenerRegistry: ListenerRegistry = null


  override def startup(configFile: String): Unit = {
    config = Config(new File(configFile))

    session = {
      val contactPoints = config.CassandraContactPoints.split(",").map(_.trim)
      val cluster = Cluster.builder().addContactPoints(contactPoints: _*).build()
      cluster.connect()
    }

    listenerRegistry = new CassandraListenerRegistry(session, config)
    leaderElection = new CassandraLeaderElection(session, config)
  }

  override def getLeaderElection = {
    Option(leaderElection).getOrElse(
      throw new IllegalStateException("LeaderElection plugin is not initialized. Call PluginLocator.startup(configFile) first"))
  }

  def getGroupMembership(groupName: String) = {
    Option(config).map(c => new CassandraGroupMembership(session, c)).getOrElse(
      throw new IllegalStateException("GroupMembership plugin is not initialized. Call PluginLocator.startup(configFile) first"))
  }

  override def getListenerRegistry = {
    Option(listenerRegistry).getOrElse(
      throw new IllegalStateException("ListenerRegistry plugin is not initialized. Call PluginLocator.startup(configFile) first"))
  }
}
