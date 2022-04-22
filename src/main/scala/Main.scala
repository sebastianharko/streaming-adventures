import cats.effect.{IO, IOApp}
import fs2.kafka._
import scala.concurrent.duration._

object Main extends IOApp.Simple {
  val run: IO[Unit] = {
    def processRecord(record: ConsumerRecord[String, String]): IO[(String, String)] = {
      IO.println("Received item!") >> IO.pure(record.key -> record.value)
    }

    val consumerSettings =
      ConsumerSettings[IO, String, String](keyDeserializer = Deserializer[IO, String],
        valueDeserializer = Deserializer[IO, String])
        .withAutoOffsetReset(AutoOffsetReset.Earliest)
        .withBootstrapServers("localhost:9092")
        .withGroupId("group-2")

    val producerSettings =
      ProducerSettings[IO, String, String]
        .withBootstrapServers("localhost:9092")

    val stream =
      KafkaConsumer.stream(consumerSettings)
        .subscribeTo("quickstart-events-3")
        .stream
        .evalMap {
          item => processRecord(item.record)
        }
    /*
      .(25) { committable =>
        processRecord(committable.record)
          .map { case (key, value) =>
            val record = ProducerRecord("quickstart-events-2", key, value)
            ProducerRecords.one(record, committable.offset)
          }
      }
      .through(KafkaProducer.pipe(producerSettings))
      .map(_.passthrough)
      .through(commitBatchWithin(500, 15.seconds))
    */

    stream.compile.drain
  }
}
