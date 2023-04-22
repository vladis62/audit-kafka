package com.example.audit.service

import com.example.audit.model.AuditKafkaMessage
import com.example.audit.serialization.AuditKafkaMessageSerializer
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.config.SaslConfigs
import org.apache.kafka.common.serialization.StringSerializer
import java.util.Properties

class KafkaProducerConfig(val bootstrap: String, val username: String, val password: String)

object KafkaProducerFactory {
    fun createKafkaProducer(config: KafkaProducerConfig): KafkaProducer<String, AuditKafkaMessage> {
        return KafkaProducer(createKafkaProps(config))
    }

    private fun createKafkaProps(config: KafkaProducerConfig): Properties {
        return Properties().apply {
            this[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG] = config.bootstrap
            this[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java
            this[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] =
                AuditKafkaMessageSerializer::class.java
            this[CommonClientConfigs.SECURITY_PROTOCOL_CONFIG] = "SASL_PLAINTEXT"
            this[SaslConfigs.SASL_MECHANISM] = "SCRAM-SHA-512"
            this[SaslConfigs.SASL_JAAS_CONFIG] =
                "org.apache.kafka.common.security.scram.ScramLoginModule required" +
                " username=${config.username} password=${config.password};"
        }
    }
}
