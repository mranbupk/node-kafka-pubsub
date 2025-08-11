class KafkaEventPublisher {
  constructor(kafkaClient) {
    this.kafka = kafkaClient;
  }

  async publishDomainEvent({ topic, entityId, resource, action, payload, traceId = null, version = 1, occurredAt = new Date().toISOString() }) {
    const envelope = { id: entityId, resource, action, payload, occurredAt, traceId, version };
    return this.kafka.produce({ topic, key: String(entityId), value: envelope, headers: traceId ? { traceId: String(traceId) } : {} });
  }
}

module.exports = KafkaEventPublisher; 