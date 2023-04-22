package com.example.audit.serialization

import com.example.audit.model.AuditKafkaMessage
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.cbor.CBORFactory
import com.fasterxml.jackson.dataformat.cbor.CBORGenerator
import org.apache.kafka.common.serialization.Serializer

class AuditKafkaMessageSerializer : Serializer<AuditKafkaMessage> {
    private val objectMapper =
        ObjectMapper(CBORFactory().enable(CBORGenerator.Feature.WRITE_TYPE_HEADER))

    override fun serialize(topic: String, data: AuditKafkaMessage): ByteArray {
        return objectMapper.writeValueAsBytes(data)
    }

    override fun configure(configs: MutableMap<String, *>?, isKey: Boolean) {
    }

    override fun close() {
    }
}
