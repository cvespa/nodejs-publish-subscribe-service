/* eslint max-classes-per-file: ["error", 5] */

import {
  AdminConfig,
  BrokersFunction,
  EachMessagePayload,
  KafkaConfig,
  ProducerBatch,
  ProducerRecord,
  RecordMetadata,
} from "kafkajs";

const kafkajs = jest.createMockFromModule<typeof import("kafkajs")>("kafkajs");

export class Admin {
  constructor(private config?: AdminConfig) {}
}

export class Consumer {
  private groupId: string;

  constructor({ groupId }: { groupId: string }) {
    this.groupId = groupId;
  }

  getGroupId() {
    return this.groupId;
  }

  // eslint-disable-next-line class-methods-use-this
  async connect() {
    return Promise.resolve();
  }

  // eslint-disable-next-line class-methods-use-this, @typescript-eslint/no-unused-vars
  async subscribe({ topic }: { topic: string }) {
    return Promise.resolve();
  }

  async run({
    eachMessage,
  }: {
    eachMessage: (payload: EachMessagePayload) => Promise<void>;
  }) {
    this.eachMessage = eachMessage;
  }

  // eslint-disable-next-line class-methods-use-this
  async disconnect() {
    return Promise.resolve();
  }

  // eslint-disable-next-line class-methods-use-this, @typescript-eslint/no-unused-vars
  async eachMessage(payload: EachMessagePayload) {
    return Promise.resolve();
  }
}

export class Logger {}

export class Producer {
  // eslint-disable-next-line class-methods-use-this
  async connect() {
    return Promise.resolve();
  }

  // eslint-disable-next-line class-methods-use-this
  async send({ topic, messages }: ProducerRecord): Promise<RecordMetadata[]> {
    const records = [];
    if (messages.length > 0) {
      for (let index = 0; index < messages.length; index += 1) {
        records.push({ topicName: topic, partition: 1, errorCode: 0 });
      }
    }

    return Promise.resolve(records);
  }

  // eslint-disable-next-line class-methods-use-this
  async sendBatch(batch: ProducerBatch): Promise<RecordMetadata[]> {
    const records = [];
    if (batch.topicMessages !== undefined && batch.topicMessages.length > 0) {
      const { topicMessages } = batch;
      for (let x = 0; x < topicMessages.length; x += 1) {
        const { topic, messages } = topicMessages[x];
        for (let y = 0; y < messages.length; y += 1) {
          records.push({ topicName: topic, partition: 1, errorCode: 0 });
        }
      }
    }

    return Promise.resolve(records);
  }

  // eslint-disable-next-line class-methods-use-this
  async disconnect() {
    return Promise.resolve();
  }
}

export class Kafka {
  private brokers: string[] | BrokersFunction;

  private clientId?: string;

  private topics: {
    [key: string]: {
      [key: string]: Array<Consumer>;
    };
  };

  constructor(config: KafkaConfig) {
    this.brokers = config.brokers;
    this.clientId = config.clientId;
    this.topics = {};
  }

  // eslint-disable-next-line class-methods-use-this
  producer() {
    return new Producer();
  }

  // eslint-disable-next-line class-methods-use-this
  consumer({ groupId }: { groupId: string }) {
    return new Consumer({
      groupId,
    });
  }

  // eslint-disable-next-line class-methods-use-this
  admin(config?: AdminConfig): Admin {
    return new Admin(config);
  }

  // eslint-disable-next-line class-methods-use-this
  logger(): Logger {
    return new Logger();
  }
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
kafkajs.Kafka = Kafka as any;

export default kafkajs;
