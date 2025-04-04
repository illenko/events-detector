package com.example.demo.streams

import com.example.demo.avro.GatewayState
import com.example.demo.avro.Payment
import com.michelin.kstreamplify.initializer.KafkaStreamsStarter
import com.michelin.kstreamplify.serde.SerdesUtils
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.kstream.Consumed
import org.apache.kafka.streams.kstream.Named
import org.apache.kafka.streams.processor.api.FixedKeyProcessor
import org.apache.kafka.streams.processor.api.FixedKeyProcessorContext
import org.apache.kafka.streams.processor.api.FixedKeyRecord
import org.apache.kafka.streams.state.KeyValueStore
import org.apache.kafka.streams.state.Stores
import org.springframework.stereotype.Component
import java.time.Duration
import java.time.Instant

@Component
class Streams : KafkaStreamsStarter() {
    override fun topology(builder: StreamsBuilder) {
        val payments =
            builder.stream("payments", Consumed.with<String, Payment>(Serdes.String(), SerdesUtils.getValueSerdes()))

        val lastActivityStoreName = "last-activity-store"
        builder.addStateStore(
            Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(lastActivityStoreName),
                Serdes.String(),
                SerdesUtils.getValueSerdes(),
            ),
        )

        payments.processValues(
            {
                object : FixedKeyProcessor<String, Payment, Payment> {
                    private lateinit var lastActivityStore: KeyValueStore<String, GatewayState>
                    private lateinit var context: FixedKeyProcessorContext<String, Payment>

                    override fun init(context: FixedKeyProcessorContext<String, Payment>) {
                        this.context = context
                        lastActivityStore = context.getStateStore(lastActivityStoreName)
                    }

                    override fun process(record: FixedKeyRecord<String, Payment>) {
                        val gatewayName: String = record.value().gateway
                        val now = Instant.now().toEpochMilli()
                        val gatewayState = lastActivityStore[gatewayName]

                        if (gatewayState == null ||
                            (now - gatewayState.lastActivityTime) >
                            Duration.ofMinutes(3).toMillis()
                        ) {
                            val newPaymentCount = 1L
                            lastActivityStore.put(gatewayName, GatewayState(now, newPaymentCount))
                            println("Gateway $gatewayName started receiving payments after 3 minutes of inactivity.")
                        } else if (gatewayState.paymentCount < 3) {
                            val newPaymentCount = gatewayState.paymentCount + 1
                            lastActivityStore.put(gatewayName, GatewayState(now, newPaymentCount))
                            if (newPaymentCount >= 3) {
                                println("Alert: Gateway $gatewayName started receiving 3+ payments after 3 minutes of inactivity.")
                            }
                        } else {
                            lastActivityStore.put(gatewayName, GatewayState(now, gatewayState.paymentCount++))
                            println("Gateway $gatewayName is still active.")
                        }

                        context.forward(record)
                    }

                    override fun close() {
                        // No-op
                    }
                }
            },
            Named.`as`("activity-processor"),
            lastActivityStoreName,
        )
    }

    override fun dlqTopic(): String = "dlq-topic"
}
