import cats.effect.{IO, IOApp}
import fs2.kafka._
import scala.concurrent.duration._
import cats.effect.std.Random
import cats.effect.std.Queue
// import cats.effect.std.
import fs2.concurrent.Topic
import cats.syntax.all._
import scala.concurrent.duration._

object Main extends IOApp.Simple {
  // val run: IO[Unit] = {
  //   def processRecord(
  //       record: ConsumerRecord[String, String]
  //   ): IO[(String, String)] = {
  //     IO.println(
  //       s"Received item - key: ${record.key}, value: ${record.value}"
  //     ) >> IO.pure(record.key -> record.value)
  //   }

  //   val consumerSettings =
  //     ConsumerSettings[IO, String, String](
  //       keyDeserializer = Deserializer[IO, String],
  //       valueDeserializer = Deserializer[IO, String]
  //     )
  //       .withAutoOffsetReset(AutoOffsetReset.Earliest)
  //       .withBootstrapServers("localhost:9092")
  //       .withGroupId("group-3")

  //   val producerSettings =
  //     ProducerSettings[IO, String, String]
  //       .withBootstrapServers("localhost:9092")

  //   val stream =
  //     KafkaConsumer
  //       .stream(consumerSettings)
  //       .subscribeTo("events-4")
  //       .stream
  //       .evalMap { item =>
  //         processRecord(item.record)
  //       }

  //     /*
  //     .(25) { committable =>
  //       processRecord(committable.record)
  //         .map { case (key, value) =>
  //           val record = ProducerRecord("quickstart-events-2", key, value)
  //           ProducerRecords.one(record, committable.offset)
  //         }
  //     }
  //     .through(KafkaProducer.pipe(producerSettings))
  //     .map(_.passthrough)
  //     .through(commitBatchWithin(500, 15.seconds))
  //      */

  //   stream.compile.drain
  // }

  val run = {

    val rnd = Random.scalaUtilRandom[IO]

    val queue = Queue.bounded[IO, Int](10)

    val topic = Topic[IO, Int]

    def producerStream(rnd: Random[IO], queue: Queue[IO, Int]) = {
      fs2.Stream
        .repeatEval(rnd.nextInt)
        .evalTap(i => IO.println(s"Producer - Generated num: $i"))
        .evalTap(queue.offer)

      // val subscriber = topic.subscribe(10).take(4)
      // subscriber.concurrently(publisher).compile.toVector
    }

    def producerStream2(rnd: Random[IO], topic: Topic[IO, Int]) = {
      fs2.Stream
        .repeatEval(rnd.nextInt)
        .evalTap(_ => IO.sleep(10.second))
        .evalTap(i => IO.println(s"Producer - Generated num: $i"))
        .through(topic.publish)

    }

    def subscriberStream(topic: Topic[IO, Int], i: Int) = {
      topic
        .subscribe(50)
        .evalMap(num => IO.println(s"Consumer $i received num: $num"))
    }

    for {
      _ <- IO.println("Starting program")
      r <- rnd
      t <- topic
      p = producerStream2(r, t).compile.drain
      s = (1 to 10).toList
        .parTraverse(i =>
          IO.println(s"Creating subscriber $i") >>
            subscriberStream(t, i).compile.drain
        )
        .void

      _ <- IO.parSequenceN(10)(List(p, s))
      _ <- IO.println("Ending")
    } yield ()
  }
}
