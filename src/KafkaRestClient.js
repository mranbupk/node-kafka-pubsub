const https = require('https');
const http = require('http');

class KafkaRestClient {
  constructor({ 
    restProxyUrl = 'http://localhost:8082',
    timeout = 30000,
    logger = null,
    headers = {}
  } = {}) {
    this.restProxyUrl = restProxyUrl.replace(/\/$/, ''); // Remove trailing slash
    this.timeout = timeout;
    this.logger = logger;
    this.defaultHeaders = {
      'Content-Type': 'application/vnd.kafka.json.v2+json',
      'Accept': 'application/vnd.kafka.v2+json',
      ...headers
    };
    this.consumers = new Map();
  }

  async init() {
    // Test connection to REST proxy
    try {
      await this.makeRequest('GET', '/');
      this.logger?.info('KafkaRestClient', 'init', 'Connected to Kafka REST Proxy', {});
    } catch (error) {
      this.logger?.error('KafkaRestClient', 'init', 'Failed to connect to REST proxy', { error: error.message });
      throw new Error(`Failed to connect to Kafka REST Proxy: ${error.message}`);
    }
  }

  async disconnect() {
    // Delete all consumer instances
    const deletePromises = [];
    for (const [consumerKey, consumerInfo] of this.consumers.entries()) {
      deletePromises.push(this.deleteConsumer(consumerInfo.groupId, consumerInfo.instanceId));
    }
    
    try {
      await Promise.all(deletePromises);
      this.consumers.clear();
      this.logger?.info('KafkaRestClient', 'disconnect', 'Disconnected all consumers', {});
    } catch (error) {
      this.logger?.error('KafkaRestClient', 'disconnect', 'Error disconnecting consumers', { error: error.message });
    }
  }

  async produce({ topic, key, value, headers = {} }, options = {}) {
    const start = Date.now();
    
    try {
      const messages = [{
        key: key,
        value: typeof value === 'string' ? value : JSON.stringify(value),
        headers: headers
      }];

      const requestBody = {
        records: messages
      };

      const result = await this.makeRequest('POST', `/topics/${topic}`, requestBody);
      const duration = Date.now() - start;
      
      this.logger?.info('KafkaRestClient', 'produce', `Produced to ${topic}`, { 
        topic, 
        key, 
        duration, 
        offsets: result.offsets 
      });
      
      return result;
    } catch (error) {
      const duration = Date.now() - start;
      this.logger?.error('KafkaRestClient', 'produce', error.message, { topic, key, duration });
      throw error;
    }
  }

  async createConsumer({ 
    groupId, 
    topics, 
    instanceId = null,
    autoOffsetReset = 'earliest',
    enableAutoCommit = true 
  }, handler, options = {}) {
    
    if (!groupId || !topics || !handler) {
      throw new Error('createConsumer requires groupId, topics, and handler');
    }

    const consumerInstanceId = instanceId || `consumer-${Date.now()}-${Math.random().toString(36).substr(2, 9)}`;
    
    try {
      // Create consumer instance
      const createConsumerBody = {
        name: consumerInstanceId,
        format: 'json',
        'auto.offset.reset': autoOffsetReset,
        'enable.auto.commit': enableAutoCommit
      };

      const consumerResponse = await this.makeRequest(
        'POST', 
        `/consumers/${groupId}`, 
        createConsumerBody
      );

      const baseUri = consumerResponse.base_uri;
      
      // Subscribe to topics
      const subscribeBody = {
        topics: Array.isArray(topics) ? topics : [topics]
      };

      await this.makeRequest('POST', `${baseUri}/subscription`, subscribeBody);

      // Store consumer info
      const consumerKey = `${groupId}-${consumerInstanceId}`;
      this.consumers.set(consumerKey, {
        groupId,
        instanceId: consumerInstanceId,
        baseUri,
        topics: subscribeBody.topics
      });

      this.logger?.info('KafkaRestClient', 'createConsumer', 'Consumer created and subscribed', {
        groupId,
        instanceId: consumerInstanceId,
        topics: subscribeBody.topics
      });

      // Start polling
      this.startPolling(consumerKey, handler, options);

      return {
        groupId,
        instanceId: consumerInstanceId,
        topics: subscribeBody.topics
      };

    } catch (error) {
      this.logger?.error('KafkaRestClient', 'createConsumer', error.message, { groupId, topics });
      throw error;
    }
  }

  async startPolling(consumerKey, handler, { 
    pollTimeoutMs = 5000, 
    maxBytes = 1048576,
    retry = { attempts: 3, delayMs: 500 } 
  } = {}) {
    
    const consumerInfo = this.consumers.get(consumerKey);
    if (!consumerInfo) {
      throw new Error(`Consumer ${consumerKey} not found`);
    }

    const poll = async () => {
      try {
        const records = await this.makeRequest(
          'GET',
          `${consumerInfo.baseUri}/records?timeout=${pollTimeoutMs}&max_bytes=${maxBytes}`
        );

        if (records && records.length > 0) {
          for (const record of records) {
            const start = Date.now();
            let attempt = 0;
            
            while (attempt < (retry?.attempts ?? 3)) {
              try {
                const decodedValue = this.safeParseJson(record.value);
                
                await handler({
                  topic: record.topic,
                  partition: record.partition,
                  offset: record.offset,
                  key: record.key,
                  value: record.value,
                  decodedValue,
                  headers: record.headers || {},
                  timestamp: record.timestamp
                });

                const duration = Date.now() - start;
                this.logger?.info('KafkaRestClient', 'consume', `Consumed from ${record.topic}`, {
                  topic: record.topic,
                  partition: record.partition,
                  offset: record.offset,
                  key: record.key,
                  duration
                });
                break;

              } catch (error) {
                attempt++;
                this.logger?.error('KafkaRestClient', 'consume', error.message, {
                  topic: record.topic,
                  partition: record.partition,
                  offset: record.offset,
                  key: record.key,
                  attempt
                });

                if (attempt >= (retry?.attempts ?? 3)) {
                  throw error;
                }
                await this.delay(retry?.delayMs ?? 500);
              }
            }
          }
        }

        // Continue polling if consumer still exists
        if (this.consumers.has(consumerKey)) {
          setImmediate(poll);
        }

      } catch (error) {
        if (this.consumers.has(consumerKey)) {
          this.logger?.error('KafkaRestClient', 'poll', error.message, { consumerKey });
          // Wait before retrying
          setTimeout(poll, 5000);
        }
      }
    };

    // Start polling
    setImmediate(poll);
  }

  async deleteConsumer(groupId, instanceId) {
    try {
      await this.makeRequest('DELETE', `/consumers/${groupId}/instances/${instanceId}`);
      this.logger?.info('KafkaRestClient', 'deleteConsumer', 'Consumer deleted', { groupId, instanceId });
    } catch (error) {
      this.logger?.error('KafkaRestClient', 'deleteConsumer', error.message, { groupId, instanceId });
    }
  }

  async makeRequest(method, path, body = null) {
    return new Promise((resolve, reject) => {
      const url = new URL(this.restProxyUrl + path);
      const isHttps = url.protocol === 'https:';
      const client = isHttps ? https : http;

      const options = {
        hostname: url.hostname,
        port: url.port || (isHttps ? 443 : 80),
        path: url.pathname + url.search,
        method: method,
        headers: { ...this.defaultHeaders },
        timeout: this.timeout
      };

      if (body) {
        const bodyStr = JSON.stringify(body);
        options.headers['Content-Length'] = Buffer.byteLength(bodyStr);
      }

      const req = client.request(options, (res) => {
        let data = '';
        res.on('data', (chunk) => data += chunk);
        
        res.on('end', () => {
          try {
            if (res.statusCode >= 200 && res.statusCode < 300) {
              const result = data ? JSON.parse(data) : {};
              resolve(result);
            } else {
              const error = data ? JSON.parse(data) : { message: `HTTP ${res.statusCode}` };
              reject(new Error(`${error.message || error.error_code} (${res.statusCode})`));
            }
          } catch (parseError) {
            reject(new Error(`Failed to parse response: ${parseError.message}`));
          }
        });
      });

      req.on('error', reject);
      req.on('timeout', () => {
        req.destroy();
        reject(new Error('Request timeout'));
      });

      if (body) {
        req.write(JSON.stringify(body));
      }
      
      req.end();
    });
  }

  safeParseJson(valueStr) {
    try {
      return JSON.parse(valueStr);
    } catch {
      return valueStr;
    }
  }

  delay(ms) { 
    return new Promise(resolve => setTimeout(resolve, ms)); 
  }

  // Utility methods for topic management
  async listTopics() {
    return await this.makeRequest('GET', '/topics');
  }

  async getTopicMetadata(topicName) {
    return await this.makeRequest('GET', `/topics/${topicName}`);
  }

  async listConsumerGroups() {
    return await this.makeRequest('GET', '/consumers');
  }
}

module.exports = KafkaRestClient;