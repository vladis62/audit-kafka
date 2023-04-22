package com.example.audit.model

data class AuditKafkaMessage(
    val auditPoint: String,
    val auditTime: Long,
    val entityId: String,
    val isError: Boolean?,
    val applicationName: String?
)
