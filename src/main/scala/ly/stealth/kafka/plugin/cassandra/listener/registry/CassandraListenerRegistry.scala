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
package ly.stealth.kafka.plugin.cassandra.listener.registry

import com.datastax.driver.core.{Row, BoundStatement, Session}
import ly.stealth.kafka.plugin.cassandra.util.Config
import org.apache.kafka.plugin.interface.{KeySetChangeListener, ValueChangeListener, ListenerRegistry}

import scala.collection.JavaConverters._

class CassandraListenerRegistry(session: Session, config: Config) extends CacheListenerRegistry(config, None) with ListenerRegistry {

  private lazy val getValueStmt = session.prepare(s"SELECT value FROM ${config.CassandraKeySpace}.kv WHERE key = ?;")

  /**
   * Register permanent callback for data change event
   * @param key the listenable data identifier
   * @param eventListener see [[ValueChangeListener]]
   */
  override def addValueChangeListener(key: String, eventListener: ValueChangeListener): Unit = {
    def fetcher = {
      val boundStatement = new BoundStatement(getValueStmt)
      val results = session.execute(boundStatement.bind(key))

      results.asScala.collectFirst { case r: Row => r.getString("value") }
    }

    addValueChangeListener(key, fetcher, eventListener)
  }

  /**
   * Register permanent callback for key-set change event
   * @param namespace the listenable key-set identifier (e.g. parent path in Zookeeper, table name in Database etc)
   * @param eventListener see [[KeySetChangeListener]]
   */
  override def addKeySetChangeListener(namespace: String, eventListener: KeySetChangeListener): Unit = {
    def fetcher = {
      val results = session.execute(s"SELECT key FROM ${config.CassandraKeySpace}." + namespace + ";")

      results.asScala.map { case r: Row => r.getString("key") }.toSet
    }

    addKeySetChangeListener(namespace, fetcher, eventListener)
  }

  /**
   * Setup everything needed for concrete implementation
   * @param context TBD. Should be abstract enough to be used by different implementations and
   *                at the same time specific because will be uniformly called from the Kafka code,
   *                regardless of the implementation
   */
  override def init(context: Any): Unit = {}

  /**
   * Release all acquired resources
   */
  override def close(): Unit = {}

}