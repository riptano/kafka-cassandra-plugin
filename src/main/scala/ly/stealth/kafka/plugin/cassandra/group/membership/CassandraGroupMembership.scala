package ly.stealth.kafka.plugin.cassandra.group.membership

import java.util.UUID
import java.util.concurrent.{ScheduledFuture, ScheduledThreadPoolExecutor, TimeUnit}

import com.datastax.driver.core.exceptions.{QueryExecutionException, WriteTimeoutException, UnavailableException}
import com.datastax.driver.core.utils.UUIDs
import com.datastax.driver.core._
import ly.stealth.kafka.plugin.cassandra.listener.registry.CacheListenerRegistry
import ly.stealth.kafka.plugin.cassandra.util.{LogUtils, Config}
import org.apache.kafka.plugin.interface.{KeySetChangeListener, GroupChangeListener, GroupMembership}

import scala.collection.JavaConverters._
import scala.collection.mutable

class CassandraGroupMembership(session: Session,
                               config: Config,
                               groupName: String = "brokers") extends GroupMembership with LogUtils {

  this.logIdent = s"[GroupMembership on group $groupName]: "

  private lazy val TryRegisterStmt = session
    .prepare(s"INSERT INTO ${config.CassandraKeySpace}.group_membership(group, id, sup_data, pid) VALUES (?, ?, ?, ?) IF NOT EXISTS;")

  private lazy val RenewMembershipStmt = session
    .prepare(s"UPDATE ${config.CassandraKeySpace}.group_membership SET sup_data = ?, pid = ? WHERE group = ? and id = ? IF pid = ?;")

  private lazy val LeaveGroupStmt = session
    .prepare(s"DELETE FROM ${config.CassandraKeySpace}.group_membership WHERE group = ? and id = ? IF pid = ?;")

  private lazy val GetGroupMembershipListStmt = session
    .prepare(s"SELECT id FROM ${config.CassandraKeySpace}.group_membership WHERE group = ?;")

  private lazy val GetGroupMembershipStmt = session
    .prepare(s"SELECT id, sup_data FROM ${config.CassandraKeySpace}.group_membership WHERE group = ?;")

  private lazy val GetGroupIdsAndPids = session
    .prepare(s"SELECT id, pid FROM ${config.CassandraKeySpace}.group_membership WHERE group = ?;")

  override def group: String = groupName

  private lazy val pid: UUID = UUIDs.timeBased()

  private def amIGroupMember(groupMember: String): Boolean = {
    val boundStatement = new BoundStatement(GetGroupIdsAndPids)
    val results = session.execute(boundStatement.bind(groupName))

    results.asScala.exists { case r: Row => r.getString("id") == groupName && r.getUUID("pid") == pid }
  }

  private def tryRegister(groupMember: String, supData: String): Boolean = {

    val boundStatement = new BoundStatement(TryRegisterStmt)
    val rs = session.execute(boundStatement.bind(groupName, groupMember, supData, pid))

    rs.wasApplied() && amIGroupMember(groupMember)
  }

  private val ex = new ScheduledThreadPoolExecutor(config.LeaderElectionExecutorThreads)

  private val renewTaskLock = new Object()
  private var renewingTaskFuture: ScheduledFuture[_] = null

  private val cacheListenerRegistry = new CacheListenerRegistry(config, Some(ex))

  private def startRenewTask(id: String, supData: String): Unit = {
    renewTaskLock.synchronized {
      renewingTaskFuture = ex.scheduleAtFixedRate(new Runnable {

        private var retiesLeft: Option[Int] = None

        private def handle(e: QueryExecutionException): Unit = {
          warn(s"Failed to renew TTL for group member $id. Operation may be retried", e)
          retiesLeft = retiesLeft.fold(Some(config.GroupMembershipRenewRetries)) {
            cur => Some(cur - 1)
          }

          retiesLeft match {
            case Some(x) if x > 0 =>
              warn(s"Renew operation will be retried during the next iteration. Retries left $retiesLeft")
            case Some(x) =>
              warn(s"Renew operation will be NOT retried as no retries left. Renewing task will be stopped")
              cancelRenewTask(id)
            // case None shouldn't happen
          }
        }

        override def run(): Unit = {
          trace(s"Renewing session for group member $id under group $groupName")
          try {
            val boundStatement = new BoundStatement(RenewMembershipStmt)
            val rs = session.execute(boundStatement.bind(supData, pid, groupName, id, pid))

            if (!rs.wasApplied()) {
              // conditional part of the query failed, there is no sense to retry:
              // this id either has been already picked by another pid or TTL has already expired and re-registration is needed
              error(s"Failed to renew TTL for group member $id due to TTL expiration, re-registration required")
              cancelRenewTask(id)
            } else {
              // this statement was successful, resetting retries counter
              retiesLeft = None
            }
          } catch {
            case ue: UnavailableException =>
              handle(ue)
            case wte: WriteTimeoutException =>
              handle(wte)
            case e: Throwable =>
              error(s"Failed to renew TTL for group member $id due to unknown error", e)
             cancelRenewTask(id)
          }
        }
      }, 0, config.LeaderElectionRenewTTLPeriod, TimeUnit.MILLISECONDS)
    }
  }

  /**
   * TODO
   * We cancel only renew task, the exception is not propagated to the client.
   * This needs to be aligned with Zookeeper behaviour
   */
  private def cancelRenewTask(id: String): Unit = {
    renewTaskLock.synchronized {
      if (renewingTaskFuture != null) {
        renewingTaskFuture.cancel(true)
        renewingTaskFuture = null
      }
    }
  }

  override def join(id: String, data: String): Unit = {
    info(s"Trying to join group member $id, leased pid is $pid")

    if (!tryRegister(id, data)) {
      throw new RuntimeException(s"Couldn't registry group member $id")
    }

    info(s"Registered group member $id successfully, starting renewing task")
    startRenewTask(id, data)
  }

  override def leave(id: String): Unit = {
    if (pid == null)
      throw new IllegalStateException("This id doesn't have assigned pid, looks like this process hasn't called join()")

    cancelRenewTask(id)
    val boundStatement = new BoundStatement(LeaveGroupStmt)
    session.execute(boundStatement.bind(groupName, id, pid))
  }

  override def membershipList(): Set[String] = {
    val boundStatement = new BoundStatement(GetGroupMembershipListStmt)
    val results = session.execute(boundStatement.bind(groupName))

    results.asScala.map { case r: Row => r.getString("id") }.toSet
  }

  override def membership(): Map[String, String] = {
    val boundStatement = new BoundStatement(GetGroupMembershipStmt)
    val results = session.execute(boundStatement.bind(groupName))

    results.asScala.map { case r: Row => (r.getString("id"), r.getString("sup_data")) }.toMap
  }

  val listeners = new mutable.HashMap[GroupChangeListener, KeySetChangeListener]()

  trait GroupChangeListenerWrapper extends GroupChangeListener {
    val original: KeySetChangeListener
  }

  override def addListener(listener: GroupChangeListener) = {

    val keySetChangeListener = new KeySetChangeListener {
      override def keySetChanged(newKeySet: Set[String]): Unit = {
        listener.onGroupChange(newKeySet)
      }
    }

    listeners.synchronized {
      listeners.put(listener, keySetChangeListener)
    }

    cacheListenerRegistry.addKeySetChangeListener("group_membership", membershipList(), keySetChangeListener)
  }

  override def removeListener(listener: GroupChangeListener) = {
    val original =
      listeners.synchronized {
        listeners.get(listener)
      }

    original.foreach {
      o =>
        cacheListenerRegistry.removeKeySetChangeListener("group_membership", o)
    }
  }

  override def init(context: Any): Unit = ???

  override def close(): Unit = ???

}
