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

import java.util.concurrent.locks.ReentrantLock
import java.util.concurrent.{ScheduledExecutorService, ScheduledFuture, ScheduledThreadPoolExecutor, TimeUnit}

import ly.stealth.kafka.plugin.cassandra.util.{Utils, LogUtils, Config}
import org.apache.kafka.plugin.interface.{KeySetChangeListener, ValueChangeListener}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
 * A key-value cache that lets registering on-change callbacks
 */
class CacheListenerRegistry(config: Config, executor: Option[ScheduledExecutorService]) extends LogUtils {

  private val lock = new ReentrantLock()

  private val valueChangeListenerMap = new mutable.HashMap[String, ListBuffer[ValueChangeListener]]
  private val keySetChangeListenerMap = new mutable.HashMap[String, ListBuffer[KeySetChangeListener]]

  private val ex = executor.getOrElse(new ScheduledThreadPoolExecutor(config.ListenerRegistryExecutorThreads))

  private val valueChangeFutures = new mutable.HashMap[String, ScheduledFuture[_]]()
  private val keySetChangeFutures = new mutable.HashMap[String, ScheduledFuture[_]]()

  class ValueWatcher(key: String, initValue: Option[String], fetcher: => Option[String]) extends Runnable {
    private var watchable = initValue

    override def run(): Unit = {
      val fetchedValue = fetcher

      if (watchable == fetchedValue) {
        // cache contains up-to-date value, do nothing
      } else {
        trace(s"About to call ${valueChangeListenerMap.get(key).size} listeners for key=$key, watchable=$watchable, fetchedValue=$fetchedValue")
        watchable = fetchedValue
        Utils.inLock(lock) {
          valueChangeListenerMap.get(key).foreach {
            listeners =>
              listeners.foreach(l => l.valueChanged(fetchedValue))
          }
        }
      }
    }
  }

  class KeySetWatcher(key: String, initValue: Set[String], fetcher: => Set[String]) extends Runnable {
    private var watchable = initValue

    override def run(): Unit = {
      val fetchedKeySet = fetcher

      if (watchable == fetchedKeySet) {
        // cache contains up-to-date value, do nothing
      } else {
        trace(s"About to call ${keySetChangeListenerMap.get(key).size} listeners for key=$key, watchable=$watchable, fetchedKeySet=$fetchedKeySet")
        watchable = fetchedKeySet
        Utils.inLock(lock) {
          keySetChangeListenerMap.get(key).foreach {
            listeners =>
              listeners.foreach(l => l.keySetChanged(fetchedKeySet))
          }
        }
      }
    }
  }

  private def attachValueChangeListeners(key: String, fetcher: => Option[String]): Unit = {
    val scheduledFuture =
      ex.scheduleAtFixedRate(new ValueWatcher(key, fetcher, fetcher), 0, config.ListenerRegistryPullPeriod, TimeUnit.MILLISECONDS)

    valueChangeFutures synchronized {
      valueChangeFutures.put(key, scheduledFuture)
    }
  }

  private def attachKeySetChangeListeners(key: String, fetcher: => Set[String]): Unit = {
    val scheduledFuture =
      ex.scheduleAtFixedRate(new KeySetWatcher(key, fetcher, fetcher), 0, config.ListenerRegistryPullPeriod, TimeUnit.MILLISECONDS)

    keySetChangeFutures synchronized {
      keySetChangeFutures.put(key, scheduledFuture)
    }
  }

  private def detachValueChangeListeners(key: String): Unit = {
    valueChangeFutures synchronized {
      valueChangeFutures.get(key) match {
        case Some(scheduledFuture) =>
          if (scheduledFuture.cancel(true)) {
            valueChangeFutures.remove(key)
          } else {
            warn(s"Failed to cancel value change observer for key $key")
          }
        case None =>

      }
    }
  }

  private def detachKeySetChangeListeners(key: String): Unit = {
    keySetChangeFutures synchronized {
      keySetChangeFutures.get(key) match {
        case Some(scheduledFuture) =>
          if (scheduledFuture.cancel(true)) {
            keySetChangeFutures.remove(key)
          } else {
            warn(s"Failed to cancel key-set change observer for key $key")
          }
        case None =>
      }
    }
  }

  def addValueChangeListener(key: String, fetcher: => Option[String], listener: ValueChangeListener): Unit = {
    Utils.inLock(lock) {
      val listeners = valueChangeListenerMap.getOrElseUpdate(key, new ListBuffer[ValueChangeListener])
      listeners.append(listener)
      // it is the first listener for this key
      if (listeners.size == 1)
        attachValueChangeListeners(key, fetcher)
    }
  }

  def removeValueChangeListener(key: String, listener: ValueChangeListener): Unit = {
    Utils.inLock(lock) {
      valueChangeListenerMap.get(key) match {
        case Some(listeners) =>
          listeners -= listener
          if (listeners.isEmpty)
            detachValueChangeListeners(key)
        case None =>
      }
    }
  }

  def addKeySetChangeListener(namespace: String, fetcher: => Set[String], listener: KeySetChangeListener): Unit = {
    Utils.inLock(lock) {
      val listeners = keySetChangeListenerMap.getOrElseUpdate(namespace, new ListBuffer[KeySetChangeListener])
      listeners.append(listener)
      // it is the first listener for this key
      if (listeners.size == 1){
        attachKeySetChangeListeners(namespace, fetcher)
      }
    }
  }

  def removeKeySetChangeListener(namespace: String, listener: KeySetChangeListener): Unit = {
    Utils.inLock(lock) {
      keySetChangeListenerMap.get(namespace) match {
        case Some(listeners) =>
          listeners -= listener
          if (listeners.isEmpty)
            detachKeySetChangeListeners(namespace)
        case None =>
      }
    }
  }
}
