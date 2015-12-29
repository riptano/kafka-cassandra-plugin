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
package ly.stealth.kafka.plugin.cassandra.util

import java.io.{File, FileInputStream, InputStream}
import java.util.Properties

import scala.util.Try

object Config extends LogUtils {

  def apply(file: File, overrides: Map[String, String]): Config = {
    val props = loadConfigFile(file)

    overrides.foreach {
      case (k, v) =>
        logger.info(s"Configuration setting is overridden: $k=$v")
        props.setProperty(k, v)
    }

    new Config(props)
  }

  def apply(file: File): Config = apply(file, Map.empty)

  private def loadConfigFile(cfg: File): Properties = {
    val props = new Properties()

    var is: InputStream = null
    try {
      is = new FileInputStream(cfg)
      props.load(is)
    } finally {
      Try(if (is != null) is.close())
    }

    props
  }
}


class Config(props: Properties) {

  val CassandraContactPoints = props.getProperty("plugin.cassandra.contact.points")
  val CassandraKeySpace = props.getProperty("plugin.cassandra.keyspace")

  val LeaderElectionExecutorThreads = props.getProperty("plugin.cassandra.le.executor.threads").toInt
  val LeaderElectionRenewTTLPeriod = props.getProperty("plugin.cassandra.le.renew.ttl.period.ms").toLong

  val GroupMembershipExecutorThreads = props.getProperty("plugin.cassandra.gm.executor.threads").toInt
  val GroupMembershipRenewTTLPeriod = props.getProperty("plugin.cassandra.gm.renew.ttl.period.ms").toLong
  val GroupMembershipRenewRetries = props.getProperty("plugin.cassandra.gm.renew.retries").toInt

  val ListenerRegistryExecutorThreads = props.getProperty("plugin.cassandra.lr.executor.threads").toInt
  val ListenerRegistryPullPeriod = props.getProperty("plugin.cassandra.lr.pull.period.ms").toLong
}
