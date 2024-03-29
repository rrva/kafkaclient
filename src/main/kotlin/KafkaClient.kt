import com.bolyartech.scram_sasl.client.ScramSaslClientProcessor
import com.bolyartech.scram_sasl.client.ScramSha512SaslClientProcessor
import io.ktor.network.selector.*
import io.ktor.network.sockets.*
import io.ktor.network.tls.*
import io.ktor.utils.io.*
import io.ktor.utils.io.core.*
import kotlinx.coroutines.*
import kotlinx.coroutines.sync.Mutex
import org.apache.kafka.common.message.*
import org.apache.kafka.common.protocol.*
import org.apache.kafka.common.record.MemoryRecords
import org.apache.kafka.common.requests.*
import org.slf4j.LoggerFactory
import java.io.Closeable
import java.io.IOException
import java.nio.ByteBuffer
import java.security.cert.X509Certificate
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import javax.net.ssl.X509TrustManager
import kotlin.coroutines.CoroutineContext

private val log = LoggerFactory.getLogger({}::class.java)

private const val version = (3).toShort()

class KafkaClient(
    private val host: String,
    private val port: Int,
    private val commandTimeoutMillis: Long = 15000L,
    private val replyTimeoutMillis: Long = 15000L,
) : CoroutineScope {

    private lateinit var apiVersions: ApiVersionsResponse

    override val coroutineContext: CoroutineContext
        get() = Dispatchers.IO + Job()

    private val serializationCache = ObjectSerializationCache()

    private val correlationId = AtomicInteger()

    private val requestTypes = LRUCache<Int, Int>(100)

    fun start() {
        startKafkaConnection()
    }

    private fun CoroutineScope.startKafkaConnection() = launch {
        try {
            val selectorManager = SelectorManager(Dispatchers.IO)
            val socket = if (port == 443 || port == 9093 || port == 19094) {
                aSocket(selectorManager).tcp()
                    .connect(host, port)
                    .tls(coroutineContext = coroutineContext) {
                        serverName = host
                        trustManager = object : X509TrustManager {
                            override fun getAcceptedIssuers(): Array<X509Certificate?> = arrayOf()
                            override fun checkClientTrusted(certs: Array<X509Certificate?>?, authType: String?) {
                                val x = 1
                            }

                            override fun checkServerTrusted(certs: Array<X509Certificate?>?, authType: String?) {
                                val x = 1
                            }
                        }
                    }

            } else {
                aSocket(selectorManager).tcp().connect(host, port)
            }
            log.info("connected to kafka ${socket.localAddress} -> ${socket.remoteAddress}")

            val writeChannel = socket.openWriteChannel(autoFlush = true)
            log.info("Opened write channel")
            val apiVersionsRequest = apiVersionsRequest()
            sendApiMessage(writeChannel, apiVersionsRequest)
            log.info("Sent api versions request")
            val readChannel = socket.openReadChannel()
            log.info("Opened read channel")
            apiVersions = readChannel.readKafkaResponse()
            log.info("Got apiversions response $apiVersions")
            val authOk = performSaslAuthentication(
                writeChannel = writeChannel,
                readChannel = readChannel,
                user = System.getenv("KAFKA_USERNAME"),
                password = System.getenv("KAFKA_PASSWORD")
            )
            if(!authOk) {
                log.error("Authentication failed")
            } else {

            }
            socket.close()
        } catch (e: Exception) {
            log.error(e.message, e)
            val x = 1
        }
    }

    private suspend fun KafkaClient.performSaslAuthentication(
        writeChannel: ByteWriteChannel,
        readChannel: ByteReadChannel,
        user: String?,
        password: String?
    ): Boolean {
        var authOk = AtomicBoolean()
        val saslHandshakeRequest = saslHandshakeRequest("SCRAM-SHA-512")
        sendApiMessage(writeChannel, saslHandshakeRequest)
        val saslHandshakeResponse = readChannel.readKafkaResponse<SaslHandshakeResponse>()
        log.info("SASL handshake response: $saslHandshakeResponse")
        val saslAuthCompletedLock = Mutex()
        val listener: ScramSaslClientProcessor.Listener = object : ScramSaslClientProcessor.Listener {
            override fun onSuccess() {
                log.info("SASL auth success")
                saslAuthCompletedLock.unlock()
                authOk.set(true)
            }

            override fun onFailure() {
                log.info("SASL auth failure")
                saslAuthCompletedLock.unlock()
                authOk.set(false)
            }
        }

        lateinit var saslClientProcessor: ScramSaslClientProcessor
        val sender = ScramSaslClientProcessor.Sender {
            log.info("About to send $it")
            val saslAuthenticateRequest = saslAuthenticateRequest(it.toByteArray())
            runBlocking {
                sendApiMessage(writeChannel, saslAuthenticateRequest)
                val saslAuthenticateResponse = readChannel.readKafkaResponse<SaslAuthenticateResponse>()
                saslClientProcessor.onMessage(saslAuthenticateResponse.saslAuthBytes().decodeToString())
            }
        }
        saslClientProcessor = ScramSha512SaslClientProcessor(
            listener, sender
        )
        saslAuthCompletedLock.lock()
        saslClientProcessor.start(user, password)
        if (withTimeoutOrNull(30000) { saslAuthCompletedLock.lock() } == null) {
            authOk.set(false)
        }
        return authOk.get()
    }

    private fun saslAuthenticateRequest(authBytes: ByteArray): SaslAuthenticateRequestData {
        val saslAuthenticateRequestData = SaslAuthenticateRequestData()
        saslAuthenticateRequestData.setAuthBytes(authBytes)
        return saslAuthenticateRequestData
    }

    private fun saslHandshakeRequest(mechanism: String): SaslHandshakeRequestData {
        val saslHandshakeRequestData = SaslHandshakeRequestData()
        saslHandshakeRequestData.setMechanism(mechanism)
        check(apiVersions.apiVersion(ApiKeys.SASL_HANDSHAKE.id).maxVersion() >= 1)
        return SaslHandshakeRequest(saslHandshakeRequestData, 1).data()

    }

    private fun saslAuth(): SaslAuthenticateRequest {
        val saslAuthenticateRequestData = SaslAuthenticateRequestData()

        return SaslAuthenticateRequest(
            saslAuthenticateRequestData,
            apiVersions.apiVersion(ApiKeys.SASL_AUTHENTICATE.id).maxVersion()
        )
    }

    private fun apiVersionsRequest(): ApiVersionsRequestData {
        val apiVersionsRequestData = ApiVersionsRequestData()
        apiVersionsRequestData.setClientSoftwareName("test")
        apiVersionsRequestData.setClientSoftwareVersion("1.0.0")
        return ApiVersionsRequest(apiVersionsRequestData, version).data()
    }

    private suspend fun sendApiMessage(writeChannel: ByteWriteChannel, apiMessage: ApiMessage) {
        val version = apiMessage.highestSupportedVersion()
        val header =
            RequestHeader(
                ApiKeys.forId(apiMessage.apiKey().toInt()),
                version,
                "test",
                correlationId.incrementAndGet()
            ).data()
        val messageSize = MessageSizeAccumulator()
        header.addSize(messageSize, serializationCache, version)
        apiMessage.addSize(messageSize, serializationCache, version)
        val bytePacketBuilder = BytePacketBuilder()
        bytePacketBuilder.writeInt(messageSize.totalSize())
        header.write(bytePacketBuilder, serializationCache, version)
        apiMessage.write(bytePacketBuilder, serializationCache, version)
        sendKafkaPacket(writeChannel, bytePacketBuilder, 10000)
    }

    private suspend fun sendKafkaPacket(
        writeChannel: ByteWriteChannel,
        bytePacketBuilder: BytePacketBuilder,
        timeoutMillis: Long
    ): Boolean {
        try {
            withTimeout(timeoutMillis) {
                writeChannel.writePacket(bytePacketBuilder.build())
            }
        } catch (te: TimeoutCancellationException) {
            log.error("Failed to send kafka command, got timeout")
            return false
        } catch (ioe: IOException) {
            log.error("Failed to send kafka command, I/O error: ${ioe.message}")
            return false
        }

        return true
    }

}

class BytePacketBuilderWritable(private val bytePacketBuilder: BytePacketBuilder) : Writable,
    Closeable {
    override fun writeByte(value: Byte) {
        bytePacketBuilder.writeByte(value)
    }

    override fun writeShort(value: Short) {
        bytePacketBuilder.writeShort(value)
    }

    override fun writeInt(value: Int) {
        bytePacketBuilder.writeInt(value)
    }

    override fun writeLong(value: Long) {
        bytePacketBuilder.writeLong(value)
    }

    override fun writeDouble(value: Double) {
        bytePacketBuilder.writeDouble(value)
    }

    override fun writeByteArray(arr: ByteArray) {
        bytePacketBuilder.writeFully(arr, 0, arr.size)
    }

    override fun writeUnsignedVarint(i: Int) {
        var value = i
        while ((value and -0x80) != 0) {
            val b = (value and 0b1111111 or 0b10000000).toByte()
            bytePacketBuilder.writeByte(b)
            value = value ushr 7
        }
        bytePacketBuilder.writeByte(value.toByte())
    }

    override fun writeByteBuffer(buf: ByteBuffer) {
        bytePacketBuilder.writeFully(buf)
    }

    override fun writeVarint(value: Int) {
        writeUnsignedVarint(value shl 1 xor (value shr 31))
    }

    override fun writeVarlong(value: Long) {
        var v: Long = value shl 1 xor (value shr 63)
        while (v and -0x80L != 0L) {
            bytePacketBuilder.writeByte((v.toInt() and 0b1111111 or 0b10000000).toByte())
            v = v ushr 7
        }
        bytePacketBuilder.writeByte(v.toByte())

    }

    override fun close() {
        bytePacketBuilder.close()
    }

}

class ByteReadPacketReadable(private val byteReadPacket: ByteReadPacket) : Readable {
    override fun readByte(): Byte {
        return byteReadPacket.readByte()
    }

    override fun readShort(): Short {
        return byteReadPacket.readShort()
    }

    override fun readInt(): Int {
        return byteReadPacket.readInt()
    }

    override fun readLong(): Long {
        return byteReadPacket.readLong()
    }

    override fun readDouble(): Double {
        return byteReadPacket.readDouble()
    }

    override fun readArray(arr: ByteArray) {
        byteReadPacket.readAvailable(arr)
    }

    override fun readUnsignedVarint(): Int {
        var value = 0
        var i = 0
        var b: Int
        while (byteReadPacket.readByte().also { b = it.toInt() }.toInt() and 0x80 != 0) {
            value = value or (b and 0x7f shl i)
            i += 7
            if (i > 28) throw IllegalArgumentException("Illegal varint: $value")
        }
        value = value or (b shl i)
        return value
    }

    override fun readByteBuffer(length: Int): ByteBuffer {
        return byteReadPacket.readByteBuffer(length)
    }

    override fun readVarint(): Int {
        val value = readUnsignedVarint()
        return value ushr 1 xor -(value and 1)
    }

    override fun readVarlong(): Long {
        var value = 0L
        var i = 0
        var b: Long
        while (readByte().also { b = it.toLong() }.toLong() and 0x80L != 0L) {
            value = value or (b and 0x7fL shl i)
            i += 7
            if (i > 63) throw IllegalArgumentException("Illegal varlong: $value")
        }
        value = value or (b shl i)
        return value ushr 1 xor -(value and 1L)

    }

    fun peek(): ByteArray {
        return byteReadPacket.copy().readBytes()
    }

}

private fun Message.write(builder: BytePacketBuilder, cache: ObjectSerializationCache, version: Short) {
    write(BytePacketBuilderWritable(builder), cache, version)
}

private suspend inline fun <reified T : AbstractResponse> ByteReadChannel.readKafkaResponse(): T {
    val messageSize = readInt(ByteOrder.BIG_ENDIAN)
    log.info("About to read a packet of $messageSize bytes")
    val packet = readPacket(messageSize)
    val packetReadable = ByteReadPacketReadable(packet)
    val apiKey = when (T::class) {
        ProduceResponse::class -> ApiKeys.PRODUCE
        FetchResponse::class -> ApiKeys.FETCH
        ListOffsetsResponse::class -> ApiKeys.LIST_OFFSETS
        MetadataResponse::class -> ApiKeys.METADATA
        LeaderAndIsrResponse::class -> ApiKeys.LEADER_AND_ISR
        StopReplicaResponse::class -> ApiKeys.STOP_REPLICA
        UpdateFeaturesResponse::class -> ApiKeys.UPDATE_METADATA
        ControlledShutdownResponse::class -> ApiKeys.CONTROLLED_SHUTDOWN
        OffsetCommitResponse::class -> ApiKeys.OFFSET_COMMIT
        OffsetFetchResponse::class -> ApiKeys.OFFSET_FETCH
        FindCoordinatorResponse::class -> ApiKeys.FIND_COORDINATOR
        JoinGroupResponse::class -> ApiKeys.JOIN_GROUP
        HeartbeatResponse::class -> ApiKeys.HEARTBEAT
        LeaveGroupResponse::class -> ApiKeys.LEAVE_GROUP
        SyncGroupResponse::class -> ApiKeys.SYNC_GROUP
        DescribeGroupsResponse::class -> ApiKeys.DESCRIBE_GROUPS
        ListGroupsResponse::class -> ApiKeys.LIST_GROUPS
        SaslHandshakeResponse::class -> ApiKeys.SASL_HANDSHAKE
        ApiVersionsResponse::class -> ApiKeys.API_VERSIONS
        CreateTopicsResponse::class -> ApiKeys.CREATE_TOPICS
        DeleteTopicsResponse::class -> ApiKeys.DELETE_TOPICS
        DeleteRecordsResponse::class -> ApiKeys.DELETE_RECORDS
        InitProducerIdResponse::class -> ApiKeys.INIT_PRODUCER_ID
        OffsetsForLeaderEpochResponse::class -> ApiKeys.OFFSET_FOR_LEADER_EPOCH
        AddPartitionsToTxnResponse::class -> ApiKeys.ADD_PARTITIONS_TO_TXN
        AddOffsetsToTxnResponse::class -> ApiKeys.ADD_OFFSETS_TO_TXN
        EndTxnResponse::class -> ApiKeys.END_TXN
        WriteTxnMarkersResponse::class -> ApiKeys.WRITE_TXN_MARKERS
        TxnOffsetCommitResponse::class -> ApiKeys.TXN_OFFSET_COMMIT
        DescribeConfigsResponse::class -> ApiKeys.DESCRIBE_CONFIGS
        AlterConfigsResponse::class -> ApiKeys.ALTER_CONFIGS
        AlterReplicaLogDirsResponse::class -> ApiKeys.ALTER_REPLICA_LOG_DIRS
        DescribeLogDirsResponse::class -> ApiKeys.DESCRIBE_LOG_DIRS
        SaslAuthenticateResponse::class -> ApiKeys.SASL_AUTHENTICATE
        CreatePartitionsResponse::class -> ApiKeys.CREATE_PARTITIONS
        CreateDelegationTokenResponse::class -> ApiKeys.CREATE_DELEGATION_TOKEN
        RenewDelegationTokenResponse::class -> ApiKeys.RENEW_DELEGATION_TOKEN
        ExpireDelegationTokenResponse::class -> ApiKeys.EXPIRE_DELEGATION_TOKEN
        DescribeDelegationTokenResponse::class -> ApiKeys.DESCRIBE_DELEGATION_TOKEN
        DeleteGroupsResponse::class -> ApiKeys.DELETE_GROUPS
        ElectLeadersResponse::class -> ApiKeys.ELECT_LEADERS
        IncrementalAlterConfigsResponse::class -> ApiKeys.INCREMENTAL_ALTER_CONFIGS
        AlterPartitionReassignmentsResponse::class -> ApiKeys.ALTER_PARTITION_REASSIGNMENTS
        ListPartitionReassignmentsResponse::class -> ApiKeys.LIST_PARTITION_REASSIGNMENTS
        OffsetDeleteResponse::class -> ApiKeys.OFFSET_DELETE
        DescribeClientQuotasResponse::class -> ApiKeys.DESCRIBE_CLIENT_QUOTAS
        AlterClientQuotasResponse::class -> ApiKeys.ALTER_CLIENT_QUOTAS
        DescribeUserScramCredentialsResponse::class -> ApiKeys.DESCRIBE_USER_SCRAM_CREDENTIALS
        AlterUserScramCredentialsResponse::class -> ApiKeys.ALTER_USER_SCRAM_CREDENTIALS
        VoteResponse::class -> ApiKeys.VOTE
        BeginQuorumEpochResponse::class -> ApiKeys.BEGIN_QUORUM_EPOCH
        EndQuorumEpochResponse::class -> ApiKeys.END_QUORUM_EPOCH
        DescribeQuorumResponse::class -> ApiKeys.DESCRIBE_QUORUM
        AlterIsrResponse::class -> ApiKeys.ALTER_ISR
        UpdateMetadataResponse::class -> ApiKeys.UPDATE_FEATURES
        EnvelopeResponse::class -> ApiKeys.ENVELOPE
        FetchSnapshotResponse::class -> ApiKeys.FETCH_SNAPSHOT
        DescribeClusterResponse::class -> ApiKeys.DESCRIBE_CLUSTER
        DescribeProducersResponse::class -> ApiKeys.DESCRIBE_PRODUCERS
        BrokerRegistrationResponse::class -> ApiKeys.BROKER_REGISTRATION
        BrokerHeartbeatResponse::class -> ApiKeys.BROKER_HEARTBEAT
        UnregisterBrokerResponse::class -> ApiKeys.UNREGISTER_BROKER
        ApiVersionsResponse::class -> ApiKeys.API_VERSIONS
        else -> throw IllegalArgumentException("Unsupported response class ${T::class}")
    }
    val responseHeader = ResponseHeaderData(packetReadable, apiKey.responseHeaderVersion(version))
    val resp = when (apiKey) {
        ApiKeys.PRODUCE -> ProduceResponse(ProduceResponseData(packetReadable, version))
        ApiKeys.FETCH -> FetchResponse<MemoryRecords>(FetchResponseData(packetReadable, version))
        ApiKeys.LIST_OFFSETS -> ListOffsetsResponse(ListOffsetsResponseData(packetReadable, version))
        ApiKeys.METADATA -> MetadataResponse(MetadataResponseData(packetReadable, version), version)
        ApiKeys.LEADER_AND_ISR -> LeaderAndIsrResponse(LeaderAndIsrResponseData(packetReadable, version), version)
        ApiKeys.STOP_REPLICA -> StopReplicaResponse(StopReplicaResponseData(packetReadable, version))
        ApiKeys.UPDATE_METADATA -> UpdateFeaturesResponse(UpdateFeaturesResponseData(packetReadable, version))
        ApiKeys.CONTROLLED_SHUTDOWN -> ControlledShutdownResponse(
            ControlledShutdownResponseData(
                packetReadable,
                version
            )
        )

        ApiKeys.OFFSET_COMMIT -> OffsetCommitResponse(OffsetCommitResponseData(packetReadable, version))
        ApiKeys.OFFSET_FETCH -> OffsetFetchResponse(OffsetFetchResponseData(packetReadable, version), version)
        ApiKeys.FIND_COORDINATOR -> FindCoordinatorResponse(FindCoordinatorResponseData(packetReadable, version))
        ApiKeys.JOIN_GROUP -> JoinGroupResponse(JoinGroupResponseData(packetReadable, version))
        ApiKeys.HEARTBEAT -> HeartbeatResponse(HeartbeatResponseData(packetReadable, version))
        ApiKeys.LEAVE_GROUP -> LeaveGroupResponse(LeaveGroupResponseData(packetReadable, version))
        ApiKeys.SYNC_GROUP -> SyncGroupResponse(SyncGroupResponseData(packetReadable, version))
        ApiKeys.DESCRIBE_GROUPS -> DescribeGroupsResponse(DescribeGroupsResponseData(packetReadable, version))
        ApiKeys.LIST_GROUPS -> ListGroupsResponse(ListGroupsResponseData(packetReadable, version))
        ApiKeys.SASL_HANDSHAKE -> SaslHandshakeResponse(SaslHandshakeResponseData(packetReadable, version))
        ApiKeys.API_VERSIONS -> ApiVersionsResponse(ApiVersionsResponseData(packetReadable, version))
        ApiKeys.CREATE_TOPICS -> CreateTopicsResponse(CreateTopicsResponseData(packetReadable, version))
        ApiKeys.DELETE_TOPICS -> DeleteTopicsResponse(DeleteTopicsResponseData(packetReadable, version))
        ApiKeys.DELETE_RECORDS -> DeleteRecordsResponse(DeleteRecordsResponseData(packetReadable, version))
        ApiKeys.INIT_PRODUCER_ID -> InitProducerIdResponse(InitProducerIdResponseData(packetReadable, version))
        ApiKeys.OFFSET_FOR_LEADER_EPOCH -> OffsetsForLeaderEpochResponse(
            OffsetForLeaderEpochResponseData(
                packetReadable,
                version
            )
        )

        ApiKeys.ADD_PARTITIONS_TO_TXN -> AddPartitionsToTxnResponse(
            AddPartitionsToTxnResponseData(
                packetReadable,
                version
            )
        )

        ApiKeys.ADD_OFFSETS_TO_TXN -> AddOffsetsToTxnResponse(AddOffsetsToTxnResponseData(packetReadable, version))
        ApiKeys.END_TXN -> EndTxnResponse(EndTxnResponseData(packetReadable, version))
        ApiKeys.WRITE_TXN_MARKERS -> WriteTxnMarkersResponse(WriteTxnMarkersResponseData(packetReadable, version))
        ApiKeys.TXN_OFFSET_COMMIT -> TxnOffsetCommitResponse(TxnOffsetCommitResponseData(packetReadable, version))
        ApiKeys.DESCRIBE_CONFIGS -> DescribeConfigsResponse(DescribeConfigsResponseData(packetReadable, version))
        ApiKeys.ALTER_CONFIGS -> AlterConfigsResponse(AlterConfigsResponseData(packetReadable, version))
        ApiKeys.ALTER_REPLICA_LOG_DIRS -> AlterReplicaLogDirsResponse(
            AlterReplicaLogDirsResponseData(
                packetReadable,
                version
            )
        )

        ApiKeys.DESCRIBE_LOG_DIRS -> DescribeLogDirsResponse(DescribeLogDirsResponseData(packetReadable, version))
        ApiKeys.SASL_AUTHENTICATE -> SaslAuthenticateResponse(SaslAuthenticateResponseData(packetReadable, version))
        ApiKeys.CREATE_PARTITIONS -> CreatePartitionsResponse(CreatePartitionsResponseData(packetReadable, version))
        ApiKeys.CREATE_DELEGATION_TOKEN -> CreateDelegationTokenResponse(
            CreateDelegationTokenResponseData(
                packetReadable,
                version
            )
        )

        ApiKeys.RENEW_DELEGATION_TOKEN -> RenewDelegationTokenResponse(
            RenewDelegationTokenResponseData(
                packetReadable,
                version
            )
        )

        ApiKeys.EXPIRE_DELEGATION_TOKEN -> ExpireDelegationTokenResponse(
            ExpireDelegationTokenResponseData(
                packetReadable,
                version
            )
        )

        ApiKeys.DESCRIBE_DELEGATION_TOKEN -> DescribeDelegationTokenResponse(
            DescribeDelegationTokenResponseData(
                packetReadable,
                version
            )
        )

        ApiKeys.DELETE_GROUPS -> DeleteGroupsResponse(DeleteGroupsResponseData(packetReadable, version))
        ApiKeys.ELECT_LEADERS -> ElectLeadersResponse(ElectLeadersResponseData(packetReadable, version))
        ApiKeys.INCREMENTAL_ALTER_CONFIGS -> IncrementalAlterConfigsResponse(
            IncrementalAlterConfigsResponseData(
                packetReadable,
                version
            )
        )

        ApiKeys.ALTER_PARTITION_REASSIGNMENTS -> AlterPartitionReassignmentsResponse(
            AlterPartitionReassignmentsResponseData(packetReadable, version)
        )

        ApiKeys.LIST_PARTITION_REASSIGNMENTS -> ListPartitionReassignmentsResponse(
            ListPartitionReassignmentsResponseData(packetReadable, version)
        )

        ApiKeys.OFFSET_DELETE -> OffsetDeleteResponse(OffsetDeleteResponseData(packetReadable, version))
        ApiKeys.DESCRIBE_CLIENT_QUOTAS -> DescribeClientQuotasResponse(
            DescribeClientQuotasResponseData(
                packetReadable,
                version
            )
        )

        ApiKeys.ALTER_CLIENT_QUOTAS -> AlterClientQuotasResponse(AlterClientQuotasResponseData(packetReadable, version))
        ApiKeys.DESCRIBE_USER_SCRAM_CREDENTIALS -> DescribeUserScramCredentialsResponse(
            DescribeUserScramCredentialsResponseData(packetReadable, version)
        )

        ApiKeys.ALTER_USER_SCRAM_CREDENTIALS -> AlterUserScramCredentialsResponse(
            AlterUserScramCredentialsResponseData(
                packetReadable,
                version
            )
        )

        ApiKeys.VOTE -> VoteResponse(VoteResponseData(packetReadable, version))
        ApiKeys.BEGIN_QUORUM_EPOCH -> BeginQuorumEpochResponse(BeginQuorumEpochResponseData(packetReadable, version))
        ApiKeys.END_QUORUM_EPOCH -> EndQuorumEpochResponse(EndQuorumEpochResponseData(packetReadable, version))
        ApiKeys.DESCRIBE_QUORUM -> DescribeQuorumResponse(DescribeQuorumResponseData(packetReadable, version))
        ApiKeys.ALTER_ISR -> AlterIsrResponse(AlterIsrResponseData(packetReadable, version))
        ApiKeys.UPDATE_FEATURES -> UpdateMetadataResponse(UpdateMetadataResponseData(packetReadable, version))
        ApiKeys.ENVELOPE -> EnvelopeResponse(EnvelopeResponseData(packetReadable, version))
        ApiKeys.FETCH_SNAPSHOT -> FetchSnapshotResponse(FetchSnapshotResponseData(packetReadable, version))
        ApiKeys.DESCRIBE_CLUSTER -> DescribeClusterResponse(DescribeClusterResponseData(packetReadable, version))
        ApiKeys.DESCRIBE_PRODUCERS -> DescribeProducersResponse(DescribeProducersResponseData(packetReadable, version))
        ApiKeys.BROKER_REGISTRATION -> BrokerRegistrationResponse(
            BrokerRegistrationResponseData(
                packetReadable,
                version
            )
        )

        ApiKeys.BROKER_HEARTBEAT -> BrokerHeartbeatResponse(BrokerHeartbeatResponseData(packetReadable, version))
        ApiKeys.UNREGISTER_BROKER -> UnregisterBrokerResponse(UnregisterBrokerResponseData(packetReadable, version))
        else -> TODO("Not implemented apiKey $apiKey")
    }
    return resp as T
}

fun main() {
    val kafkaClient = KafkaClient(System.getenv(("KAFKA_HOST")), 19094)
    runBlocking {
        kafkaClient.start()
    }
    Thread.sleep(Long.MAX_VALUE)
}