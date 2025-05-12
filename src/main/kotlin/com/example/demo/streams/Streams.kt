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

        val stateStoreName = "payment-activity-store"
        builder.addStateStore(
            Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(stateStoreName),
                Serdes.String(),
                SerdesUtils.getValueSerdes(),
            ),
        )

        payments.processValues(
            {
                object : FixedKeyProcessor<String, Payment, Payment> {
                    private lateinit var stateStore: KeyValueStore<String, GatewayState>
                    private lateinit var context: FixedKeyProcessorContext<String, Payment>

                    override fun init(context: FixedKeyProcessorContext<String, Payment>) {
                        this.context = context
                        stateStore = context.getStateStore(stateStoreName)
                    }

                    override fun process(record: FixedKeyRecord<String, Payment>) {
                        val payment = record.value()
                        val now = Instant.now().toEpochMilli()
                        val weekInMillis = Duration.ofDays(7).toMillis()
                        val hourInMillis = Duration.ofHours(1).toMillis()

                        val gatewayKey = "gateway:${payment.gateway}"
                        val operationKey = "operation:${payment.operation}"
                        val compositeKey = "composite:${payment.gateway}:${payment.currency}:${payment.operation}"

                        checkFirstPaymentAfterInactivity(gatewayKey, now, weekInMillis) {
                            println("First payment for gateway ${payment.gateway} after a week of inactivity")
                        }

                        checkFirstPaymentAfterInactivity(operationKey, now, weekInMillis) {
                            println("First payment for operation ${payment.operation} after a week of inactivity")
                        }

                        checkHighFrequencyPayments(
                            compositeKey,
                            now,
                            hourInMillis,
                            weekInMillis,
                            payment
                        )

                        context.forward(record)
                    }

                    private fun checkFirstPaymentAfterInactivity(
                        key: String,
                        now: Long,
                        inactivityPeriod: Long,
                        onFirstPayment: () -> Unit
                    ) {
                        val state = stateStore[key]
                        if (state == null || (now - state.lastActivityTime) > inactivityPeriod) {
                            stateStore.put(key, GatewayState(now, 1, "", "", 1))
                            onFirstPayment()
                        } else {
                            stateStore.put(
                                key, updateState(
                                    state = state,
                                    lastActivityTime = now
                                )
                            )
                        }
                    }

                    private fun checkHighFrequencyPayments(
                        key: String,
                        now: Long,
                        hourPeriod: Long,
                        inactivityPeriod: Long,
                        payment: Payment
                    ) {
                        val state = stateStore[key]
                        if (state == null) {
                            stateStore.put(
                                key,
                                GatewayState(now, 1, payment.currency, payment.operation, 1)
                            )
                        } else {
                            val timeDiff = now - state.lastActivityTime
                            when {
                                timeDiff > inactivityPeriod -> {
                                    // Reset after week of inactivity
                                    stateStore.put(
                                        key,
                                        GatewayState(now, 1, payment.currency, payment.operation, 1)
                                    )
                                }

                                timeDiff <= hourPeriod -> {
                                    // Within the hour window
                                    val newCount = state.hourlyPaymentCount + 1
                                    stateStore.put(
                                        key,
                                        updateState(
                                            state = state,
                                            lastActivityTime = now,
                                            hourlyPaymentCount = newCount
                                        )
                                    )
                                    if (newCount > 10) {
                                        println(
                                            "Alert: More than 10 payments in an hour detected for " +
                                                    "gateway: ${payment.gateway}, " +
                                                    "currency: ${payment.currency}, " +
                                                    "operation: ${payment.operation}"
                                        )
                                    }
                                }

                                else -> {
                                    // Reset hour counter for new hour period
                                    stateStore.put(
                                        key,
                                        GatewayState(now, 1, payment.currency, payment.operation, 1)
                                    )
                                }
                            }
                        }
                    }

                    override fun close() {
                        // No-op
                    }
                }
            },
            Named.`as`("payment-activity-processor"),
            stateStoreName,
        )

    }

    override fun dlqTopic(): String = "dlq-topic"

    private fun updateState(
        state: GatewayState,
        lastActivityTime: Long,
        hourlyPaymentCount: Long? = null
    ): GatewayState {
        return GatewayState(
            lastActivityTime,
            state.paymentCount,
            state.currency,
            state.operation,
            hourlyPaymentCount ?: state.hourlyPaymentCount
        )
    }
}
