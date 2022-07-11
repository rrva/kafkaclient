import io.kotest.common.runBlocking
import io.kotest.core.spec.style.FunSpec
import io.kotest.extensions.embedded.kafka.embeddedKafkaListener
import kotlinx.coroutines.delay
import org.slf4j.LoggerFactory
import kotlin.time.ExperimentalTime

@OptIn(ExperimentalTime::class)
class EmbeddedKafkaListenerTest : FunSpec({

    listener(embeddedKafkaListener)

    val log = LoggerFactory.getLogger("test")

    test("api version request") {

       log.info("embeddedKafkaListener ${embeddedKafkaListener.host}:${embeddedKafkaListener.port}")

        /*val consumer = embeddedKafkaListener.stringStringConsumer("foo")
        consumer.poll(Duration.ofMillis(100))
        */
        val kafkaClient = KafkaClient(embeddedKafkaListener.host, embeddedKafkaListener.port)
        runBlocking {
            delay(1000)
            kafkaClient.start()
            delay(1000)
        }

    }

})