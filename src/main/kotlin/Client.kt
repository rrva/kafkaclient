import kotlin.jvm.JvmStatic
import com.bolyartech.scram_sasl.client.ScramSaslClientProcessor
import com.bolyartech.scram_sasl.client.ScramSha256SaslClientProcessor

object Client {
    @JvmStatic
    fun main(args: Array<String>) {
        val listener: ScramSaslClientProcessor.Listener = object : ScramSaslClientProcessor.Listener {
            override fun onSuccess() {}
            override fun onFailure() {}
        }
        val sender = ScramSaslClientProcessor.Sender { }
        val processor: ScramSaslClientProcessor = ScramSha256SaslClientProcessor(
            listener, sender
        )
    }
}