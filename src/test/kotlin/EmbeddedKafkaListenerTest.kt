import io.kotest.assertions.timing.eventually
import io.kotest.core.spec.style.FunSpec
import io.kotest.extensions.embedded.kafka.embeddedKafkaListener
import io.kotest.matchers.shouldBe
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import org.apache.kafka.clients.producer.ProducerRecord
import org.slf4j.LoggerFactory
import java.time.Duration
import kotlin.time.Duration.Companion.seconds
import kotlin.time.ExperimentalTime

@OptIn(ExperimentalTime::class)
class EmbeddedKafkaListenerTest : FunSpec({

    listener(embeddedKafkaListener)

    val log = LoggerFactory.getLogger("test")

    test("api version request") {

       log.info("embeddedKafkaListener ${embeddedKafkaListener.host}:${embeddedKafkaListener.port}")

        val consumer = embeddedKafkaListener.stringStringConsumer("foo")
        consumer.poll(Duration.ofMillis(100))
        val kafkaClient = KafkaClient(embeddedKafkaListener.host, embeddedKafkaListener.port)
        runBlocking {
            delay(10000)
            kafkaClient.sendApiRequest()
            delay(10000)
        }

    }

})