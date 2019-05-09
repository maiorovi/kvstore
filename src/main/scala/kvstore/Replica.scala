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
  
  var kv = Map.empty[String, String]
  // a map from secondary replicas to replicators
  var secondaries = Map.empty[ActorRef, ActorRef]
  // the current set of replicators
  var replicators = Set.empty[ActorRef]

  var sequences = Map.empty[String, Int]
  // task to replicator
  var persistenceTasks = Map.empty[Int, (ActorRef, Persist)]

  arbiter ! Join

  private val persister: ActorRef = context.actorOf(persistenceProps)


  def receive = {
    case JoinedPrimary   => context.become(leader)
    case JoinedSecondary =>
      context.system.scheduler.schedule(FiniteDuration(0, TimeUnit.MILLISECONDS),
        FiniteDuration(100, TimeUnit.MILLISECONDS), new Runnable {
          override def run(): Unit = persistenceTasks.foreach(p => {
            persister ! p._2._2
          })
        })
      context.become(replica)
  }

  /* TODO Behavior for  the leader role. */
  val leader: Receive = {
    case Insert(key, value, id) =>
      kv += (key -> value)
      sender() ! OperationAck(id)
    case get: Get => handleGet(get)
    case Remove(key, id) =>
      kv = kv.-(key)
      sender() ! OperationAck(id)
    case _ =>
  }

  /* TODO Behavior for the replica role. */
  val replica: Receive = {
    case get: Get => handleGet(get)
    case snapshot: Snapshot => handleSnapshot(snapshot)
    case persisted: Persisted =>
      val replicator = persistenceTasks(persisted.id.toInt)
      persistenceTasks -= persisted.id.toInt
      replicator._1 ! SnapshotAck(persisted.key, persisted.id)
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
      case Some(value) => kv += (key -> value)
        sequences += (key -> (seq + 1))
        val persistMessage = Persist(key, Some(value), seq)
        persister ! persistMessage
        val tuple = (sender(), persistMessage)
        persistenceTasks += (seq -> tuple)
      case None => kv = kv.-(key)
        sequences += (key -> (seq + 1))
        val persistMessage = Persist(key, None, seq)
        persister ! persistMessage
        val tuple = (sender(), persistMessage)
        persistenceTasks += (seq -> tuple)
    }
  }

  private def handleGet(get: Get): Unit = {
    val key = get.key
    sender() ! GetResult(key, kv.get(key), get.id)
  }

}

