import akka.actor.{Actor, Props}
import akka.io.Tcp
import akka.util.ByteString

import scala.collection.concurrent.TrieMap
import scala.collection.mutable
import scala.collection.mutable.{Map, TreeSet}

/**
 * Created by whogben on 5/27/16.
 */
object Redis {
  def props(): Props =
    Props(new Redis())

  val valueMap: TrieMap[String, Value] = TrieMap[String, Value]()
  val scoredValueMap = TrieMap[String, TreeSet[ScoredValue]]()
}

case class Value(value: String, expire: Long = 0)
case class ScoredValue(value: String, score: Long) extends Ordered[ScoredValue] {
  override def compare(that: ScoredValue): Int = {
    if (this.score == that.score) {
      this.value.compare(that.value)
    } else if (this.score > that.score) {
      1
    } else {
      -1
    }
  }
}

class Redis extends Actor {

  val SET = """^SET\s+([a-zA-Z0-9-_]+)\s+([a-zA-Z0-9-_]+)$""".r
  val SETEX = """^SET\s+([a-zA-Z0-9-_]+)\s+([a-zA-Z0-9-_]+)\s+EX\s+([0-9]+)$""".r
  val GET = """^GET\s+([a-zA-Z0-9-_]+)$""".r
  val DEL = """^DEL\s+([a-zA-Z0-9-_]+\s)*([a-zA-Z0-9-_]+)$""".r
  val DBSIZE = """^DBSIZE$""".r
  val INCR = """^INCR\s+([a-zA-Z0-9-_]+)$""".r
  val ZADD = """^ZADD\s+([a-zA-Z0-9-_]+)\s+([a-zA-Z0-9-_]+)\s+([a-zA-Z0-9-_]+)$""".r
  val ZCARD = """^ZCARD\s+([a-zA-Z0-9-_]+)$""".r
  val ZRANK = """^ZRANK\s+([a-zA-Z0-9-_]+)\s+([a-zA-Z0-9-_]+)$""".r
  val ZRANGE = """^ZRANGE\s+([a-zA-Z0-9-_]+)\s+([a-zA-Z0-9-_]+)\s+([a-zA-Z0-9-_]+)$""".r

  def receive: Receive = {
    case Tcp.Received(data) =>
      val text = data.utf8String.trim
      text match {
        case SET(key, value) => sender ! write(set(key, value))
        case SETEX(key, value, ex) => sender ! write(set(key, value))
        case GET(key) => sender ! write(get(key))
        case DEL(_, _) => sender ! write(del(text))
        case DBSIZE() => sender ! write(dbSize())
        case ZADD(key, score, member) => sender ! write(zadd(key, score, member))
        case ZCARD(key) => sender ! write(zcard(key))
        case ZRANK(key, member) => sender ! write(zrank(key, member))
        case ZRANGE(key, start, stop) => sender ! write(zrange(key, start, stop))
        case _ => sender ! write("INVALID INPUT")
      }
  }

  private def write(message: String) = {
    Tcp.Write(ByteString(s"$message\n"))
  }

  private def timeStamp: Long = System.currentTimeMillis / 1000

  def set(key: String, value: String): String = {
    Redis.valueMap += (key -> Value(value))
    "OK"
  }

  def zadd(key: String, score: String, member: String): String = {
    val treeSet: mutable.TreeSet[ScoredValue] = Redis.scoredValueMap.getOrElse(key, mutable.TreeSet[ScoredValue]())
    treeSet += ScoredValue(member, score.toLong)
    Redis.scoredValueMap += key -> treeSet
    "(interger) 1"
  }

  def zcard(key: String): String = {
    "(interger) " + Redis.scoredValueMap.get(key).map(_.size).getOrElse(0)
  }

  def zrank(key: String, member: String): String = {
    Redis.scoredValueMap.get(key).map { set => search(member, set.iterator, 0) }.getOrElse("(nil)")
  }

  def zrange(key: String, start: String, stop: String): String = {
    if (start.toInt > stop.toInt) {
      "(nil)"
    } else {
      Redis.scoredValueMap.get(key).map { set =>
        if (Math.abs(start.toInt) > set.size) {
          "(nil)"
        } else {
          val numDrop = if (start.toInt < 0) {
            set.size + start.toInt
          } else {
            start.toInt
          }
          val stopNum = if (stop.toInt < 0) {
            set.size + stop.toInt
          } else {
            stop.toInt
          }
          printN(set.drop(numDrop).iterator, stopNum - numDrop)
        }
      }.getOrElse("(nil)")
    }
  }

  def printN(iterator: Iterator[ScoredValue], n: Int): String = {
    if (!iterator.hasNext) ""
    else {
      val next = iterator.next()
      "\"" + next.value + "\"\n" + printN(iterator, n - 1)
    }
  }

  private def search(member: String, iterator: Iterator[ScoredValue], index: Int): String = {
    if (!iterator.hasNext) {
      "(nil)"
    } else if (iterator.next().value == member) {
      "(integer) " + index
    } else {
      search(member, iterator, index + 1)
    }
  }

  def dbSize(): String = {
    "(integer)" + (Redis.valueMap.size + Redis.scoredValueMap.size)
  }

  def setEx(key: String, value: String, expire: String): String = {
    val expireLong = expire.toLong
    Redis.valueMap += (key -> Value(value, timeStamp + expireLong))
    "OK"
  }

  def get(key: String): String = {
    Redis.valueMap.get(key).map { value =>
      if (value.expire != 0 && value.expire > timeStamp) {
        Redis.valueMap -= key
        "(nil)"
      } else {
        value.value
      }
    }.getOrElse("(nil)")
  }

  def del(commandString: String): String = {
    val keys = commandString.stripPrefix("DEL").trim.replaceAll("""\s+""", " ").split(' ')
    val deleted = keys.foldLeft(0) { (count, key) =>
      if (Redis.valueMap.contains(key)) {
        Redis.valueMap -= key
        count + 1
      } else {
        count
      }
    }
    "(integer) " + deleted
  }

}
