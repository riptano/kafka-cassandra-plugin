Kafka Cassandra Plugin
======================

This is an implementation for [KIP-30](https://cwiki.apache.org/confluence/display/KAFKA/KIP-30+-+Allow+for+brokers+to+have+plug-able+consensus+and+meta+data+storage+sub+systems) - That is in progress being built to allow for brokers to have plug-able consensus and meta data storage sub systems. 

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