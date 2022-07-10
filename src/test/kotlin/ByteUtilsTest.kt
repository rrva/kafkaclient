import io.ktor.utils.io.core.*
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Assertions.assertArrayEquals
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.io.IOException
import java.nio.ByteBuffer

class ByteUtilsTest {
    private val x00: Byte = 0x00
    private val x01: Byte = 0x01
    private val x02: Byte = 0x02
    private val x0F: Byte = 0x0f
    private val x07: Byte = 0x07
    private val x08: Byte = 0x08
    private val x3F: Byte = 0x3f
    private val x40: Byte = 0x40
    private val x7E: Byte = 0x7E
    private val x7F: Byte = 0x7F
    private val xFF = 0xff.toByte()
    private val x80 = 0x80.toByte()
    private val x81 = 0x81.toByte()
    private val xBF = 0xbf.toByte()
    private val xC0 = 0xc0.toByte()
    private val xFE = 0xfe.toByte()
    @Test
    @Throws(Exception::class)
    fun testUnsignedVarintSerde() {
        assertUnsignedVarintSerde(0, byteArrayOf(x00))
        assertUnsignedVarintSerde(-1, byteArrayOf(xFF, xFF, xFF, xFF, x0F))
        assertUnsignedVarintSerde(1, byteArrayOf(x01))
        assertUnsignedVarintSerde(63, byteArrayOf(x3F))
        assertUnsignedVarintSerde(-64, byteArrayOf(xC0, xFF, xFF, xFF, x0F))
        assertUnsignedVarintSerde(64, byteArrayOf(x40))
        assertUnsignedVarintSerde(8191, byteArrayOf(xFF, x3F))
        assertUnsignedVarintSerde(-8192, byteArrayOf(x80, xC0, xFF, xFF, x0F))
        assertUnsignedVarintSerde(8192, byteArrayOf(x80, x40))
        assertUnsignedVarintSerde(-8193, byteArrayOf(xFF, xBF, xFF, xFF, x0F))
        assertUnsignedVarintSerde(1048575, byteArrayOf(xFF, xFF, x3F))
        assertUnsignedVarintSerde(1048576, byteArrayOf(x80, x80, x40))
        assertUnsignedVarintSerde(Int.MAX_VALUE, byteArrayOf(xFF, xFF, xFF, xFF, x07))
        assertUnsignedVarintSerde(Int.MIN_VALUE, byteArrayOf(x80, x80, x80, x80, x08))
    }

    @Test
    @Throws(Exception::class)
    fun testVarintSerde() {
        assertVarintSerde(0, byteArrayOf(x00))
        assertVarintSerde(-1, byteArrayOf(x01))
        assertVarintSerde(1, byteArrayOf(x02))
        assertVarintSerde(63, byteArrayOf(x7E))
        assertVarintSerde(-64, byteArrayOf(x7F))
        assertVarintSerde(64, byteArrayOf(x80, x01))
        assertVarintSerde(-65, byteArrayOf(x81, x01))
        assertVarintSerde(8191, byteArrayOf(xFE, x7F))
        assertVarintSerde(-8192, byteArrayOf(xFF, x7F))
        assertVarintSerde(8192, byteArrayOf(x80, x80, x01))
        assertVarintSerde(-8193, byteArrayOf(x81, x80, x01))
        assertVarintSerde(1048575, byteArrayOf(xFE, xFF, x7F))
        assertVarintSerde(-1048576, byteArrayOf(xFF, xFF, x7F))
        assertVarintSerde(1048576, byteArrayOf(x80, x80, x80, x01))
        assertVarintSerde(-1048577, byteArrayOf(x81, x80, x80, x01))
        assertVarintSerde(134217727, byteArrayOf(xFE, xFF, xFF, x7F))
        assertVarintSerde(-134217728, byteArrayOf(xFF, xFF, xFF, x7F))
        assertVarintSerde(134217728, byteArrayOf(x80, x80, x80, x80, x01))
        assertVarintSerde(-134217729, byteArrayOf(x81, x80, x80, x80, x01))
        assertVarintSerde(Int.MAX_VALUE, byteArrayOf(xFE, xFF, xFF, xFF, x0F))
        assertVarintSerde(Int.MIN_VALUE, byteArrayOf(xFF, xFF, xFF, xFF, x0F))
    }

    @Test
    @Throws(Exception::class)
    fun testVarlongSerde() {
        assertVarlongSerde(0, byteArrayOf(x00))
        assertVarlongSerde(-1, byteArrayOf(x01))
        assertVarlongSerde(1, byteArrayOf(x02))
        assertVarlongSerde(63, byteArrayOf(x7E))
        assertVarlongSerde(-64, byteArrayOf(x7F))
        assertVarlongSerde(64, byteArrayOf(x80, x01))
        assertVarlongSerde(-65, byteArrayOf(x81, x01))
        assertVarlongSerde(8191, byteArrayOf(xFE, x7F))
        assertVarlongSerde(-8192, byteArrayOf(xFF, x7F))
        assertVarlongSerde(8192, byteArrayOf(x80, x80, x01))
        assertVarlongSerde(-8193, byteArrayOf(x81, x80, x01))
        assertVarlongSerde(1048575, byteArrayOf(xFE, xFF, x7F))
        assertVarlongSerde(-1048576, byteArrayOf(xFF, xFF, x7F))
        assertVarlongSerde(1048576, byteArrayOf(x80, x80, x80, x01))
        assertVarlongSerde(-1048577, byteArrayOf(x81, x80, x80, x01))
        assertVarlongSerde(134217727, byteArrayOf(xFE, xFF, xFF, x7F))
        assertVarlongSerde(-134217728, byteArrayOf(xFF, xFF, xFF, x7F))
        assertVarlongSerde(134217728, byteArrayOf(x80, x80, x80, x80, x01))
        assertVarlongSerde(-134217729, byteArrayOf(x81, x80, x80, x80, x01))
        assertVarlongSerde(Int.MAX_VALUE.toLong(), byteArrayOf(xFE, xFF, xFF, xFF, x0F))
        assertVarlongSerde(Int.MIN_VALUE.toLong(), byteArrayOf(xFF, xFF, xFF, xFF, x0F))
        assertVarlongSerde(17179869183L, byteArrayOf(xFE, xFF, xFF, xFF, x7F))
        assertVarlongSerde(-17179869184L, byteArrayOf(xFF, xFF, xFF, xFF, x7F))
        assertVarlongSerde(17179869184L, byteArrayOf(x80, x80, x80, x80, x80, x01))
        assertVarlongSerde(-17179869185L, byteArrayOf(x81, x80, x80, x80, x80, x01))
        assertVarlongSerde(2199023255551L, byteArrayOf(xFE, xFF, xFF, xFF, xFF, x7F))
        assertVarlongSerde(-2199023255552L, byteArrayOf(xFF, xFF, xFF, xFF, xFF, x7F))
        assertVarlongSerde(2199023255552L, byteArrayOf(x80, x80, x80, x80, x80, x80, x01))
        assertVarlongSerde(-2199023255553L, byteArrayOf(x81, x80, x80, x80, x80, x80, x01))
        assertVarlongSerde(281474976710655L, byteArrayOf(xFE, xFF, xFF, xFF, xFF, xFF, x7F))
        assertVarlongSerde(-281474976710656L, byteArrayOf(xFF, xFF, xFF, xFF, xFF, xFF, x7F))
        assertVarlongSerde(281474976710656L, byteArrayOf(x80, x80, x80, x80, x80, x80, x80, x01))
        assertVarlongSerde(-281474976710657L, byteArrayOf(x81, x80, x80, x80, x80, x80, x80, 1))
        assertVarlongSerde(36028797018963967L, byteArrayOf(xFE, xFF, xFF, xFF, xFF, xFF, xFF, x7F))
        assertVarlongSerde(-36028797018963968L, byteArrayOf(xFF, xFF, xFF, xFF, xFF, xFF, xFF, x7F))
        assertVarlongSerde(36028797018963968L, byteArrayOf(x80, x80, x80, x80, x80, x80, x80, x80, x01))
        assertVarlongSerde(-36028797018963969L, byteArrayOf(x81, x80, x80, x80, x80, x80, x80, x80, x01))
        assertVarlongSerde(4611686018427387903L, byteArrayOf(xFE, xFF, xFF, xFF, xFF, xFF, xFF, xFF, x7F))
        assertVarlongSerde(-4611686018427387904L, byteArrayOf(xFF, xFF, xFF, xFF, xFF, xFF, xFF, xFF, x7F))
        assertVarlongSerde(4611686018427387904L, byteArrayOf(x80, x80, x80, x80, x80, x80, x80, x80, x80, x01))
        assertVarlongSerde(-4611686018427387905L, byteArrayOf(x81, x80, x80, x80, x80, x80, x80, x80, x80, x01))
        assertVarlongSerde(Long.MAX_VALUE, byteArrayOf(xFE, xFF, xFF, xFF, xFF, xFF, xFF, xFF, xFF, x01))
        assertVarlongSerde(Long.MIN_VALUE, byteArrayOf(xFF, xFF, xFF, xFF, xFF, xFF, xFF, xFF, xFF, x01))
    }

    @Test
    fun testInvalidVarint() {
        val buf = buildPacket {
            writeFully(byteArrayOf(xFF, xFF, xFF, xFF, xFF, x01))
        }
        Assertions.assertThrows(IllegalArgumentException::class.java) { ByteReadPacketReadable(buf).readVarint() }
    }

    @Test
    fun testInvalidVarlong() {
        // varlong encoding has one overflow byte
        val buf = buildPacket {
            writeFully(byteArrayOf(xFF, xFF, xFF, xFF, xFF, xFF, xFF, xFF, xFF, xFF, x01))
        }
        Assertions.assertThrows(IllegalArgumentException::class.java) { ByteReadPacketReadable(buf).readVarlong() }
    }

    @Test
    @Throws(IOException::class)
    fun testDouble() {
        assertDoubleSerde(0.0, 0x0L)
        assertDoubleSerde(1.0, 0x3FF0000000000000L)
        assertDoubleSerde(-1.0, -0x4010000000000000L)
        assertDoubleSerde(123e45, 0x49B58B82C0E0BB00L)
        assertDoubleSerde(-123e45, -0x364a747d3f1f4500L)
        assertDoubleSerde(Double.MIN_VALUE, 0x1L)
        assertDoubleSerde(-Double.MIN_VALUE, -0x7fffffffffffffffL)
        assertDoubleSerde(Double.MAX_VALUE, 0x7FEFFFFFFFFFFFFFL)
        assertDoubleSerde(-Double.MAX_VALUE, -0x10000000000001L)
        assertDoubleSerde(Double.NaN, 0x7FF8000000000000L)
        assertDoubleSerde(Double.POSITIVE_INFINITY, 0x7FF0000000000000L)
        assertDoubleSerde(Double.NEGATIVE_INFINITY, -0x10000000000000L)
    }

    @Throws(IOException::class)
    private fun assertUnsignedVarintSerde(value: Int, expectedEncoding: ByteArray) {
        val buf = BytePacketBuilder()
        val bw = BytePacketBuilderWritable(buf)
        bw.writeUnsignedVarint(value)
        val x = buf.build()
        val y = x.copy()
        val a = x.readBytes()
        assertArrayEquals(expectedEncoding, a)
        assertEquals(value, ByteReadPacketReadable(y).readUnsignedVarint())
    }

    @Throws(IOException::class)
    private fun assertVarintSerde(value: Int, expectedEncoding: ByteArray) {
        val buf = BytePacketBuilder()
        val bw = BytePacketBuilderWritable(buf)
        bw.writeVarint(value)
        val x = buf.build()
        val y = x.copy()
        val actual = x.readBytes()
        assertArrayEquals(expectedEncoding, actual)
        assertEquals(value, ByteReadPacketReadable(y).readVarint())

    }

    fun ByteBuffer.toArray(): ByteArray {
        val dest = ByteArray(this.remaining())
        if (this.hasArray()) {
            System.arraycopy(this.array(), this.position() + this.arrayOffset(), dest, 0, dest.size)
        } else {
            val pos = this.position()
            this.position(pos)
            this[dest]
            this.position(pos)
        }
        return dest
    }

    @Throws(IOException::class)
    private fun assertVarlongSerde(value: Long, expectedEncoding: ByteArray) {
        val buf = BytePacketBuilder()
        val bw = BytePacketBuilderWritable(buf)
        bw.writeVarlong(value)
        val x = buf.build()
        val y = x.copy()
        assertArrayEquals(expectedEncoding, x.readBytes())
        assertEquals(value, ByteReadPacketReadable(y).readVarlong() )

    }

    @Throws(IOException::class)
    private fun assertDoubleSerde(value: Double, expectedLongValue: Long) {
        val buf = BytePacketBuilder()
        val bw = BytePacketBuilderWritable(buf)
        bw.writeDouble(value)
        assertEquals(expectedLongValue, buf.preview {it.readLong()})
        assertEquals(value, buf.preview { ByteReadPacketReadable(it).readDouble() })
    }
}