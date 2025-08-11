const { Kafka, logLevel } = require('kafkajs');

class KafkaClient {
  constructor({ brokers, clientId, ssl = false, sasl = null, logger = null } = {}) {
    if (!brokers || !clientId) {
      throw new Error('KafkaClient requires brokers and clientId');
    }
    this.logger = logger;
    this.kafka = new Kafka({ brokers: Array.isArray(brokers) ? brokers : [brokers], clientId, ssl, sasl, logLevel: logLevel.NOTHING });
    this.producer = this.kafka.producer({ allowAutoTopicCreation: true });
    this.consumers = new Map();
  }

  async init() {
    await this.producer.connect();
    this.logger?.info('KafkaClient', 'init', 'Producer connected', {});
  }

  async disconnect() {
    try {
      for (const consumer of this.consumers.values()) {
        await consumer.disconnect();
      }
    } finally {
      await this.producer.disconnect();
    }
    this.logger?.info('KafkaClient', 'disconnect', 'Disconnected', {});
  }

  async produce({ topic, key, value, headers = {} }, options = {}) {
    const start = Date.now();
    try {
      const messages = [{ key, value: typeof value === 'string' ? value : JSON.stringify(value), headers }];
      const result = await this.producer.send({ topic, messages, acks: -1, ...options });
      const duration = Date.now() - start;
      this.logger?.info('KafkaClient', 'produce', `Produced to ${topic}`, { topic, key, duration, result });
      return result;
    } catch (error) {
      const duration = Date.now() - start;
      this.logger?.error('KafkaClient', 'produce', error, { topic, key, duration });
      throw error;
    }
  }

  async createConsumer({ groupId, topics, fromBeginning = false }, handler, { retry = { attempts: 3, delayMs: 500 } } = {}) {
    if (!groupId || !topics || !handler) {
      throw new Error('createConsumer requires groupId, topics, handler');
    }
    const consumer = this.kafka.consumer({ groupId });
    await consumer.connect();
    for (const topic of (Array.isArray(topics) ? topics : [topics])) {
      await consumer.subscribe({ topic, fromBeginning });
    }

    await consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        const start = Date.now();
        const key = message.key?.toString();
        const valueStr = message.value?.toString();
        const headers = Object.fromEntries(Object.entries(message.headers || {}).map(([k, v]) => [k, v?.toString()]));
        let attempt = 0;
        while (true) {
          try {
            const payload = this.safeParseJson(valueStr);
            await handler({ topic, partition, message, decodedValue: payload, headers, key });
            const duration = Date.now() - start;
            this.logger?.info('KafkaClient', 'consume', `Consumed from ${topic}`, { topic, partition, key, offset: message.offset, duration });
            break;
          } catch (error) {
            attempt += 1;
            this.logger?.error('KafkaClient', 'consume', error, { topic, partition, key, offset: message.offset, attempt });
            if (attempt >= (retry?.attempts ?? 3)) {
              throw error;
            }
            await this.delay(retry?.delayMs ?? 500);
          }
        }
      }
    });

    this.consumers.set(groupId, consumer);
    this.logger?.info('KafkaClient', 'createConsumer', 'Consumer running', { groupId, topics });
    return consumer;
  }

  safeParseJson(valueStr) {
    try {
      return JSON.parse(valueStr);
    } catch {
      return valueStr;
    }
  }

  delay(ms) { return new Promise(res => setTimeout(res, ms)); }
}

module.exports = KafkaClient; 