package kvstore

import java.util.concurrent.TimeUnit

import akka.actor.{Actor, ActorRef, OneForOneStrategy, Props}
import kvstore.Arbiter._

import scala.collection.immutable.Queue
import akka.actor.SupervisorStrategy.Restart

import scala.annotation.tailrec
import akka.pattern.{ask, pipe}
import akka.actor.Terminated

import scala.concurrent.duration._
import akka.actor.PoisonPill
import akka.actor.OneForOneStrategy
import akka.actor.SupervisorStrategy
import akka.util.Timeout

object Replica {
  sealed trait Operation {
    def key: String
    def id: Long
  }
  case class Insert(key: String, value: String, id: Long) extends Operation
  case class Remove(key: String, id: Long) extends Operation
  case class Get(key: String, id: Long) extends Operation

  sealed trait OperationReply
  case class OperationAck(id: Long) extends OperationReply
  case class OperationFailed(id: Long) extends OperationReply
  case class GetResult(key: String, valueOption: Option[String], id: Long) extends OperationReply

  def props(arbiter: ActorRef, persistenceProps: Props): Props = Props(new Replica(arbiter, persistenceProps))
}

class Replica(val arbiter: ActorRef, persistenceProps: Props) extends Actor {
  import Replica._
  import Replicator._
  import Persistence._
  import context.dispatcher

  /*
   * The contents of this actor is just a suggestion, you can implement it in any way you like.
   */
  
  var kv = Map.empty[String, (String, Int)]
  // a map from secondary replicas to replicators
  var secondaries = Map.empty[ActorRef, ActorRef]
  // the current set of replicators
  var replicators = Set.empty[ActorRef]

  var sequences = Map.empty[String, Int]
  // task to replicator
  var persistenceTasks = Map.empty[Int, PersistenceAttempt]
  var replicationTracking = Map.empty[Int, ReplicationTrackerInfo]

  case class ReplicationTrackerInfo(val client: ActorRef, var ackedReplication: Int, target: Int) {
    def isReplicationReady(): Boolean = ackedReplication == target - 1

    def incrementReplicationCount(): Unit = ackedReplication += 1
  }

  case class PersistenceAttempt(client: ActorRef, persist: Persist, var attempt: Int) {
    def incrementAttempt():Unit = {
      attempt += 1
    }
  }

  arbiter ! Join

  private val persister: ActorRef = context.actorOf(persistenceProps)


  def receive = {
    case JoinedPrimary   =>
      context.system.scheduler.schedule(FiniteDuration(0, TimeUnit.MILLISECONDS),
        FiniteDuration(100, TimeUnit.MILLISECONDS), new Runnable {
          override def run(): Unit = persistenceTasks.foreach(p => {
            if (p._2.attempt == 10) {
              p._2.client ! OperationFailed(p._2.persist.id)
            } else {
              p._2.incrementAttempt()
              persister ! p._2.persist
            }
          })
        })
      context.become(leader)

    case JoinedSecondary =>
      context.system.scheduler.schedule(FiniteDuration(0, TimeUnit.MILLISECONDS),
        FiniteDuration(100, TimeUnit.MILLISECONDS), new Runnable {
          override def run(): Unit = persistenceTasks.foreach(p => {
            persister ! p._2.persist
          })
        })
      context.become(replica)
  }

  /* TODO Behavior for  the leader role. */
  val leader: Receive = {
    case Replicas(replicas) =>
      val set = secondaries.keySet.
        filter(r => !replicas.contains(r))
      secondaries --= set

      set.foreach( r => {
        context.stop(r)
      })

      replicas.foreach(secondary => {
        if (!secondaries.contains(secondary) && secondary != context.self) {
          val replicator = context.actorOf(Props(classOf[Replicator], secondary))
          secondaries += (secondary -> replicator)
          kv.foreach(entry => {
            val id = entry._2._2
            replicationTracking += (id -> ReplicationTrackerInfo(ActorRef.noSender, 0, 1))
            println(">>> replicating " + entry)
            replicator ! Replicate(entry._1, Some(entry._2._1), id)
          })
        }
      })

      replicators = secondaries.values.toSet
    case Insert(key, value, id) =>
      val t =(value, id.toInt)
      kv += (key -> t)
      val persistMessage = Persist(key, Some(value), id)
      persister ! persistMessage
      val tuple = PersistenceAttempt(sender(), persistMessage, 0)
      persistenceTasks += (id.toInt -> tuple)
    case get: Get => handleGet(get)
    case Remove(key, id) =>
      kv = kv.-(key)
      val persistMessage = Persist(key, None, id)
      val tuple = PersistenceAttempt(sender(), persistMessage, 0)
      persistenceTasks += (id.toInt -> tuple)
      persister ! persistMessage
    case persisted: Persisted =>
      val persistenceAttempt = persistenceTasks(persisted.id.toInt)
      persistenceTasks -= persisted.id.toInt
      val storedValue = kv.get(persisted.key)
      if (replicators.isEmpty) {
        persistenceAttempt.client ! OperationAck(persisted.id)
      } else {
        replicators.foreach( r => r ! Replicate(persisted.key, storedValue.map(_._1), persisted.id))
        replicationTracking += (persisted.id.toInt ->
          ReplicationTrackerInfo(persistenceAttempt.client, 0, replicators.size))
              val z = context.system.scheduler.scheduleOnce(FiniteDuration(1, TimeUnit.SECONDS),new Runnable {
                override def run(): Unit = if (replicationTracking.contains(persisted.id.toInt)) {
                  replicationTracking -= persisted.id.toInt
                  persistenceAttempt.client ! OperationFailed(persisted.id)
                }})
      }
    case Replicated(key, id) =>
      val o = replicationTracking(id.toInt)
      println(s"replicated received $o")
      println(s"replicated received ${o.isReplicationReady()}")
      if (o.isReplicationReady()) {
        replicationTracking -= id.toInt
        if (o.client != null) {
          println(s"sending ack ${OperationAck(id)}")
          o.client ! OperationAck(id)
        }
      } else {
        o.incrementReplicationCount()
      }
    case _ =>
  }

  /* TODO Behavior for the replica role. */
  val replica: Receive = {
    case get: Get => handleGet(get)
    case snapshot: Snapshot => handleSnapshot(snapshot)
    case persisted: Persisted =>
      val persistenceAttempt = persistenceTasks(persisted.id.toInt)
      persistenceTasks -= persisted.id.toInt
      persistenceAttempt.client ! SnapshotAck(persisted.key, persisted.id)
    case _ =>
  }

  private def handleSnapshot(snapshot: Snapshot) : Unit  = {
    val key = snapshot.key
    val snapshotSeq = snapshot.seq.toInt

    sequences.get(key) match {
      case Some(seq) => if (seq == snapshotSeq) {
        doHandleSnapshot(snapshot, snapshotSeq)
      } else if (seq > snapshot.seq) {
        sender() ! SnapshotAck(key, snapshotSeq)
      }
      case None => if (snapshot.seq == 0) {
        doHandleSnapshot(snapshot, 0)

      }
    }
  }

  private def doHandleSnapshot(snapshot: Snapshot, seq: Int) : Unit = {
    val key = snapshot.key
    val snapshotSeq = snapshot.seq

    snapshot.valueOption match {
      case Some(value) =>
        val t = (value, snapshot.seq.toInt)
        kv += (key -> t)
        sequences += (key -> (seq + 1))
        val persistMessage = Persist(key, Some(value), seq)
        persister ! persistMessage
        val tuple = PersistenceAttempt(sender(), persistMessage, 0)
        persistenceTasks += (seq -> tuple)
      case None => kv = kv.-(key)
        sequences += (key -> (seq + 1))
        val persistMessage = Persist(key, None, seq)
        persister ! persistMessage
        val tuple = PersistenceAttempt(sender(), persistMessage, 0)
        persistenceTasks += (seq -> tuple)
    }
  }

  private def handleGet(get: Get): Unit = {
    val key = get.key
    sender() ! GetResult(key, kv.get(key).map(_._1), get.id)
  }

}

