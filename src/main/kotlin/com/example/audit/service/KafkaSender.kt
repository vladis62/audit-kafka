package com.example.audit.service

import com.example.audit.model.AuditKafkaMessage
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.header.Headers
import java.io.Closeable
import java.util.concurrent.Future

class KafkaSender(private val config: KafkaProducerConfig) : Closeable {
    private val producer by lazy {
        KafkaProducerFactory.createKafkaProducer(config)
    }

    fun send(
        topic: String?,
        partition: Int? = null,
        key: String?,
        value: AuditKafkaMessage?,
        headers: Headers? = null
    ): RecordMetadata {
        try {
            val futureRecord: Future<RecordMetadata> = producer
                .send(ProducerRecord(topic, partition, key, value, headers))
            return futureRecord.get()
        } catch (e: Exception) {
            val errMsg = String.format(
                "Error send to Kafka topic %s for ApplicationId: %s",
                topic,
                "appId"
            )
            println(errMsg)
            throw e
        }
    }

    override fun close() {
        producer.close()
    }
}
