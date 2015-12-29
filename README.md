Kafka Cassandra Plugin
======================

This is an implementation for [KIP-30](https://cwiki.apache.org/confluence/display/KAFKA/KIP-30+-+Allow+for+brokers+to+have+plug-able+consensus+and+meta+data+storage+sub+systems) - That is in progress being built to allow for brokers to have plug-able consensus and meta data storage sub systems. 

It uses the patch under [KAFKA-2921](https://issues.apache.org/jira/browse/KAFKA-2921) to run it in.

# Motivation
Kafka Brokers today rely on Apache Zookeeper. Many folks in the community have expressed a desire to either change the zkclient and start using Apache Curator or allowing other systems like etcd, consul, Apache Cassandra and others to handle the role Zookeeper is currently playing. By allowing the brokers to have both a way to plug-in another server for storing the meta data and also for leader election then we can have the ability to-do this.

# Proposed Changes

This KIP proposes approach for isolating coordination related functionality to separate modules. These modules should come with a public interface that can have pluggable implementations.
Zookeeper has advanced low-level primitives for coordinating distributed systems – ephemeral nodes, key-value storage, watchers. Such concepts may not be available in other consensus frameworks. At the same time such low-level primitives (especially ephemeral nodes) are error prone and usually a cause of subtle bugs in Kafka coordination code.
That's why instead of focusing on question “how Kafka does coordination with Zookeeper” it is proposed to concentrate on question “what general problems of distributed systems are solved in Kafka by means of Zookeeper”. Having defined interface boundaries this way, we'll be able to hide implementation details under concrete realizations developed with corresponding built-in facilities available in particular tools (e.g. ephemeral nodes vs TTLs).
It is proposed to separate such high-level concerns:
* Group membership protocol (Kafka brokers form a cluster; consumer connectors form a consumer group)
* Leader election (electing controller among brokers)
* Distributed key-value storage (topic config storage etc etc etc)
* Data-change listeners (triggering events - partition reassignment, catching up isr-s etc)

# Public Interfaces
Below each module is presented by its interface.

*Group Membership*
```
/**
 * A connector for group membership protocol. Supports two modes:
 * 1) "joining" (becoming the member, leaving the group, subscribing to change notifications)
 * 2) "observing" (fetching group state, subscribing to change notifications)
 *
 */
trait GroupMembershipClient {
  /**
   * Each instance of this class is tightly coupled with exactly one group,
   * once set (during initialization) cannot be changed
   * @return unique group identifier among all application groups
   */
  def group: String
 
  /**
   * Become a member of this group. Throw an exception in case of ID conflict
   * @param id unique member identifier among members of this group
   * @param data supplemental data to be stored along with member ID
   */
  def join(id: String, data: String): Unit
 
  /**
   * Stop membership in this group
   * @param id unique member identifier among members of this group
   */
  def leave(id: String): Unit
 
  /**
   * Fetch membership of this group
   * @return IDs of the members of this group
   */
  def membershipList(): Set[String]
 
  /**
   * Fetch detailed membership of this group
   * @return IDs and corresponding supplemental data of the members of this group
   */
  def membership(): Map[String, String]
 
  /**
   * A callback fired on event
   */
  trait Listener {
    /**
     * Event fired when the group membership has changed (member(s) joined and/or left)
     * @param membership new membership of the group
     */
    def onGroupChange(membership: Set[String])
  }
 
  /**
   * Register permanent on group change listener.
   * There is no guarantee listener will be fired on ALL events (due to session reconnects etc)
   * @param listener see [[Listener]]
   */
  def addListener(listener: Listener)
 
  /**
   * Deregister on group change listener
   * @param listener see [[Listener]]
   */
  def removeListener(listener: Listener)
 
  /**
   * Setup everything needed for concrete implementation
   * @param context TBD. Should be abstract enough to be used by different implementations and
   *                at the same time specific because will be uniformly called from the Kafka code -
   *                regardless of the implementation
   */
  def init(context: Any): Unit
 
  /**
   * Release all acquired resources
   */
  def close(): Unit
}
```
*Leader Election*
```
/**
 * A connector for leadership election protocol. Supports two modes:
 * 1) "running for election" (joining the candidates for leadership, resigning as a leader, subscribing to change notifications)
 * 2) "observing" (getting current leader, subscribing to change notifications)
 *
 */
trait LeaderElectionClient{
  /**
   * Each instance of this class is tightly coupled with leadership over exactly one service (resource),
   * once set (during initialization) cannot be changed
   *
   * @return unique group identifier among all application services (resources)
   */
  def service: String
 
  /**
   * Get current leader of the resource (if any)
   * @return the leader id if it exists
   */
  def getLeader: Option[String]
 
  /**
   * Make this candidate eligible for leader election and try to obtain leadership for this service if it's vacant
   *
   * @param candidate ID of the candidate which is eligible for election
   * @return true if given candidate is now a leader
   */
  def nominate(candidate: String): Boolean
 
  /**
   * Voluntarily resign as a leader and initiate new leader election.
   * It's a client responsibility to stop all leader duties before calling this method to avoid more-than-one-leader cases
   *
   * @param leader current leader ID (will be ignored if not a leader)
   */
  def resign(leader: String): Unit
 
  /**
   * A callback fired on leader change event
   */
  trait Listener {
    /**
     * Event fired when the leader has changed (resigned or acquired a leadership)
     * @param leader new leader for the given service if one has been elected, otherwise None
     */
    def onLeaderChange(leader: Option[String])
  }
 
  /**
   * Register permanent on leader change listener
   * There is no guarantee listener will be fired on ALL events (due to session reconnects etc)
   * @param listener see [[Listener]]
   */
  def addListener(listener: Listener)
  /**
   * Deregister on leader change listener
   * @param listener see [[Listener]]
   */
  def removeListener(listener: Listener)
 
  /**
   * Setup everything needed for concrete implementation
   * @param context TBD. Should be abstract enough to be used by different implementations and
   *                at the same time specific because will be uniformly called from the Kafka code -
   *                regardless of the implementation
   */
  def init(context: Any): Unit
 
  /**
   * Release all acquired resources
   */
  def close(): Unit
}
```
*Storage*
```
/**
 * Interface to a (persistent) key value storage
 */
trait Storage {
  /**
   * Get data by its key
   * @param key data ID in this storage
   * @return future result of the value (if exists) associated with the key
   */
  def fetch(key: String): Future[Option[String]]
 
  /**
   * Persist value with its associated key. The contract is to throw an exception
   * if such key already exists
   *
   * @param key data ID in this storage
   * @param data value associated with the key
   */
  def put(key: String, data: String)
 
  /**
   * Update value by its associated key. The contract is to throw an exception
   * if such key doesn't exist
   *
   * @param key data ID in this storage
   * @param data value associated with the key
   */
  def update(key: String, data: String)
 
  /**
   * Setup everything needed for concrete implementation
   * @param context TBD. Should be abstract enough to be used by different implementations and
   *                at the same time specific because will be uniformly called from the Kafka code -
   *                regardless of the implementation
   */
  def init(context: Any): Unit
 
  /**
   * Release all acquired resources
   */
  def close(): Unit
}
```
*Listener Registry*
```
/**
 * A registry for async data change notifications
 */
trait ListenerRegistry {
  /**
   * Register permanent callback for data change event
   * @param key the listenable data identifier
   * @param eventListener see [[ValueChangeListener]]
   */
  def addValueChangeListener(key: String, eventListener: ValueChangeListener): Unit
 
  /**
   * Deregister permanent callback for data change event
   * @param key the listenable data identifier
   * @param eventListener see [[EventListener]]
   * @tparam T type of the data ID
   */
  def removeValueChangeListener(key: String, eventListener: ValueChangeListener): Unit
 
  /**
   * Register permanent callback for key-set change event
   * @param namespace the listenable key-set identifier (e.g. parent path in Zookeeper, table name in Database etc)
   * @param eventListener see [[ValueChangeListener]]
   */
  def addKeySetChangeListener(namespace: String, eventListener: KeySetChangeListener): Unit
 
  /**
   * Deregister permanent callback for key-set change event
   * @param namespace the listenable key-set identifier (e.g. parent path in Zookeeper, table name in Database etc)
   * @param eventListener see [[ValueChangeListener]]
   */
  def removeKeySetChangeListener(namespace: String, eventListener: KeySetChangeListener): Unit
 
  /**
   * Setup everything needed for concrete implementation
   * @param context TBD. Should be abstract enough to be used by different implementations and
   *                at the same time specific because will be uniformly called from the Kafka code,
   *                regardless of the implementation
   */
  def init(context: Any): Unit
 
  /**
   * Release all acquired resources
   */
  def close(): Unit
}
 
/**
 * Callback on value change event
 */
trait ValueChangeListener {
  def valueChanged(newValue: Option[String])
}
 
/**
 * Callback on key-set change event
 */
trait KeySetChangeListener {
  def keySetChanged(newKeySet: Set[String])
}
```

# How to run

To try these changes (at the moment only leader election module is available) you will need to patch upstream Kafka project 
(patch is provided separately, see details below), build this project and change Kafka configuration file and classpath so it
can use this jar as a plugin, then start Kafka as usual.

Step by step instructions:

I. Create Cassandra tables

1. You will need 3 tables: `leader_election`, `kv` and `group_membership` located in `kafka_cluster_1` Cassandra keyspace (keyspace is regulated by 
the `plugin.cassandra.keyspace` setting in plugin.properties)

In cqlsh execute (change replication factor (`X`) per your needs, note that all queries are executed with CL QUORUM,
which means you will need `n/2 + 1` replicas to be alive. E.g. if you have 3 nodes Cassandra cluster, set `X` to 3, this
way you will tolerate 1 node loss):

```
  CREATE KEYSPACE kafka_cluster_1
    WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : X };

  USE kafka_cluster_1;

  CREATE TABLE leader_election (
       resource text PRIMARY KEY,
       owner text,
       sup_data text
  ) with default_time_to_live = 2;
 
  CREATE TABLE kv (
       key text PRIMARY KEY,
       value text
  );
  
  CREATE TABLE group_membership (
         group text,
         id text,
         sup_data text,
         pid timeuuid,
         PRIMARY KEY (group, id)
    ) with default_time_to_live = 2;
``` 

II. Building

2. Patch Kafka so it supports plug-able consensus and storage modules

```
 # cd $WORKING_DIR
 # git clone https://github.com/apache/kafka.git kafka-with-plugins
 # cd kafka-with-plugins
 # git reset --hard a5382a
 # curl https://issues.apache.org/jira/secure/attachment/12776574/KIP-30-LE-WIP.patch > KIP-30-LE-WIP.patch
 # git am KIP-30-LE-WIP.patch
```

3. Install Kafka jars to local maven repo so plug-able module interfaces (defined in Kafka `plugin-interface` sub-project) are available
for this project (kafka-cassandra-plugin)

```
  # cd $WORKING_DIR/kafka-with-plugins
  # ./gradlew -PscalaVersion=2.11 install -x signArchives
```

4. Build Kafka distribution archive

```
  # cd $WORKING_DIR/kafka-with-plugins
  # ./gradlew -PscalaVersion=2.11 releaseTarGz -x signArchives
```
 
5. Build this project jar

```
 # cd $WORKING_DIR
 # git clone https://github.com/riptano/kafka-cassandra-plugin.git kafka-cassandra-plugin
 # switch the branch if needed
 # cd kafka-with-plugins
 # ./gradlew jar
```

III. Running

Note: It is advisable you have _Oracle_ JDK installed (starting from version 6)

6. Un-pack Kafka release archive and copy plugin jar and config file to the kafka working directory

```
 # cd $RUNNING_DIR
 # cp $WORKING_DIR/core/build/... . 
 # tar -xf kafka_2.11-0.9.1.0-SNAPSHOT.tgz
 # mkdir kafka_2.11-0.9.1.0-SNAPSHOT/plugin
 # cp $WORKING_DIR/kafka-cassandra-plugin/plugin.jar $RUNNING_DIR/kafka_2.11-0.9.1.0-SNAPSHOT/plugin
 # cp $WORKING_DIR/kafka-cassandra-plugin/src/main/resource/plugin.properties $RUNNING_DIR/kafka_2.11-0.9.1.0-SNAPSHOT/config
```

7. Specify your plugin metadata in Kafka `server.properties`

Add these lines to the `$RUNNING_DIR/kafka_2.11-0.9.1.0-SNAPSHOT/config/server.properties` 

(fix `plugin.configuration.file` setting to full path of the `plugin.properties` file on your running environment)

```
plugin.additional.jars=plugin/plugin.jar
plugin.locator.classname=ly.stealth.kafka.plugin.cassandra.CassandraPluginLocator
plugin.configuration.file=/opt/apache/kafka/config/plugin.properties
```

8. Specify you Cassandra cluster location (coma separated list of Cassandra host names) in `plugin.properties`

Search for `plugin.cassandra.contact.points=` entry in `$RUNNING_DIR/kafka_2.11-0.9.1.0-SNAPSHOT/config/plugin.properties`

9. Enhance logging (optional, helpful for troubleshooting)

Add these lines to `$RUNNING_DIR/kafka_2.11-0.9.1.0-SNAPSHOT/config/log4j.properties`

```
# Plugins configuration
log4j.logger.ly.stealth.kafka.plugin.cassandra=DEBUG, kafkaAppender
log4j.additivity.ly.stealth.kafka.plugin.cassandra=false
log4j.logger.ly.stealth.kafka.plugin.cassandra.leader.election=TRACE, controllerAppender
log4j.additivity.ly.stealth.kafka.plugin.cassandra.leader.election=false
```

10. Start Kafka

E.g. (you may need sudo rights):

```
  # $RUNNING_DIR/kafka_2.11-0.9.1.0-SNAPSHOT/bin/kafka-server-start.sh $RUNNING_DIR/kafka_2.11-0.9.1.0-SNAPSHOT/config/server.properties 1>> /tmp/broker.log 2>> /tmp/broker.log &
```
