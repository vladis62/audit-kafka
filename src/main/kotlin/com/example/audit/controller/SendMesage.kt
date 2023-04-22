package com.example.audit.controller

import com.example.audit.model.AuditKafkaMessage
import com.example.audit.service.KafkaProducerConfig
import com.example.audit.service.KafkaSender
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController
import java.time.LocalDateTime
import java.time.ZoneId
import kotlin.random.Random

@RestController
@RequestMapping("/audit")
class SendMessage {

    private val startTime = currentTime()

    @PostMapping
    fun log() {
        for (i in 0..100_000) {
            AuditKafkaMessage(
                auditPoint = if (i in 0..50000) "Service1.Request" else "Service1.Response",
                entityId = "${i % 100}",
                applicationName = if (i in 0..50000) "Service1" else "Service2",
                isError = Random.nextInt() % 10 == 0,
                auditTime = startTime + i * 1000,
            ).sendToKafka()
        }
    }

    companion object {
        private val config by lazy {
            KafkaProducerConfig(
                bootstrap = "localhost:9092",
                username = "kafka",
                password = "kafka"
            )
        }
        private val kafkaSender by lazy {
            KafkaSender(config)
        }

        fun AuditKafkaMessage.sendToKafka(
            topic: String = "logger.records.store",
            key: String = "uuid"
        ) {
            kafkaSender.send(
                topic = topic,
                key = key,
                value = this
            )
        }
    }

    private final fun currentTime() = LocalDateTime
        .now()
        .atZone(ZoneId.of("Europe/Moscow"))
        .toInstant()
        .toEpochMilli()
}