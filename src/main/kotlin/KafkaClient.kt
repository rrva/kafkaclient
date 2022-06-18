import io.ktor.network.selector.*
import io.ktor.network.sockets.*
import io.ktor.utils.io.*
import io.ktor.utils.io.core.*
import kotlinx.coroutines.*
import org.slf4j.LoggerFactory
import java.io.IOException
import java.util.*
import kotlin.coroutines.CoroutineContext

class KafkaClient(
    private val host: String,
    private val port: Int,
    private val commandTimeoutMillis: Long = 15000L,
    private val replyTimeoutMillis: Long = 15000L,
) : CoroutineScope {

    override val coroutineContext: CoroutineContext
        get() = Dispatchers.IO + Job()

    private val log = LoggerFactory.getLogger(this::class.java)

    fun sendApiRequest() {
        startKafkaConnection()
    }

    private fun CoroutineScope.startKafkaConnection() = launch {
        val socket = aSocket(ActorSelectorManager(Dispatchers.IO)).tcp().connect(InetSocketAddress(host, port))
        log.info("connected to kafka ${socket.localAddress} -> ${socket.remoteAddress}")

        val writeChannel = socket.openWriteChannel(autoFlush = true)
        val readChannel = socket.openReadChannel()
        sendKafkaRequest(writeChannel, ApiVersionRequest("consumer-test_consumer_group_1655575623452-1", "apache-kafka-java", "2.8.0"), 10000)
        val firstReplyByte = readChannel.readInt(ByteOrder.BIG_ENDIAN)
        log.info("firstReplyByte: $firstReplyByte")
        val remaining = readChannel.readRemaining(firstReplyByte.toLong())
        log.info("resp: "+HexFormat.of().formatHex(remaining.readBytes()))
        socket.close()
    }

    private suspend fun sendKafkaRequest(
        writeChannel: ByteWriteChannel,
        msg: KafkaMessage,
        timeoutMillis: Long
    ): Boolean {
        try {
            withTimeout(timeoutMillis) {
                writeChannel.writePacket(msg.toPacket())
                log.debug("Sent command")
            }
        } catch (te: TimeoutCancellationException) {
            log.error("Failed to send kafka command ${msg}, got timeout")
            return false
        } catch (ioe: IOException) {
            log.error("Failed to send kafka command ${msg}, I/O error: ${ioe.message}")
            return false
        }

        return true
    }
}

fun BytePacketBuilder.writeKafkaHeader(header: KafkaHeader) {
    writeInt(header.messageLength)
    writeShort(header.apiKey)
    writeShort(header.apiVersion)
    writeInt(header.correlationId)
}

data class KafkaHeader(val messageLength: Int, val apiKey: Short, val apiVersion: Short, val correlationId: Int) 
sealed class KafkaMessage() {
   abstract fun toPacket(): ByteReadPacket
}
class ApiVersionRequest(private val clientId: String, private val clientSoftwareName: String, private val clientSoftwareVersion: String): KafkaMessage() {
    private fun writePacket(builder: BytePacketBuilder) {
        builder.writeNullableString(clientId)
        builder.writeCompactString(clientSoftwareName)
        builder.writeCompactString(clientSoftwareVersion)
        builder.writeByte(0)
    }
    
    override fun toPacket(): ByteReadPacket {
        val builder = BytePacketBuilder()
        writePacket(builder)
        val headerSize = 2+2+4
        val messageSize = builder.size + headerSize
        builder.reset()
        val header = KafkaHeader(messageSize, 18, 3, 1)
        builder.writeKafkaHeader(header)
        writePacket(builder)
        return builder.build()
    }
}

private fun BytePacketBuilder.writeNullableString(text: String) {
    writeShort(text.length.toShort())
    writeText(text)
    writeByte(0)
}


private fun BytePacketBuilder.writeCompactString(text: String) {
    writeByte((text.length + 1).toByte())
    writeText(text)
}

fun main() {
    val kafkaClient = KafkaClient("127.0.0.1", 9092)
    runBlocking {
        kafkaClient.sendApiRequest()
        delay(10000)
    }

}