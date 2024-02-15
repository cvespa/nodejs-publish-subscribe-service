import {
  Consumer,
  ConsumerConfig,
  ConsumerRunConfig,
  ConsumerSubscribeTopics,
  Kafka,
  Producer,
  ProducerRecord,
  RecordMetadata,
} from "kafkajs";

export class PublishSubscribeService {
  protected kafka: Kafka;

  protected consumer?: Consumer;

  protected producer?: Producer;

  constructor(
    private brokers: string[] | string,
    private clientId?: string,
  ) {
    if (!Array.isArray(this.brokers)) {
      this.brokers = this.brokers.split(",");
    }

    this.kafka = new Kafka({
      clientId: this.clientId,
      brokers: this.brokers,
    });
  }

  async disconnect() {
    const promises: Promise<void>[] = [];
    if (this.consumer) {
      promises.push(this.consumer.disconnect());
    }

    if (this.producer) {
      promises.push(this.producer.disconnect());
    }

    await Promise.all(promises);
  }

  setupProducer() {
    this.producer = this.kafka.producer();

    return this.producer;
  }

  async publish(record: ProducerRecord): Promise<RecordMetadata[]> {
    let { producer } = this;
    if (producer === undefined) {
      producer = this.setupProducer();
    }

    await producer.connect();
    return producer.send(record);
  }

  setupConsumer(config: ConsumerConfig) {
    this.consumer = this.kafka.consumer(config);

    return this.consumer;
  }

  async subscribe(
    subscription: ConsumerSubscribeTopics,
    consumerRunConfig: ConsumerRunConfig,
  ): Promise<void>;
  async subscribe(
    config: ConsumerConfig,
    subscription: ConsumerSubscribeTopics,
    consumerRunConfig: ConsumerRunConfig,
  ): Promise<void>;
  async subscribe(
    first: ConsumerConfig | ConsumerSubscribeTopics,
    second: ConsumerSubscribeTopics | ConsumerRunConfig,
    third?: ConsumerRunConfig,
  ): Promise<void> {
    let config: ConsumerConfig | undefined;
    let subscription: ConsumerSubscribeTopics | undefined;
    let consumerRunConfig: ConsumerRunConfig | undefined;

    if ("topics" in first) {
      subscription = first;
    } else {
      config = first;
    }

    if ("eachMessage" in second || "eachBatch" in second) {
      consumerRunConfig = second;
    } else if ("topics" in second) {
      subscription = second;
    }

    if (third !== undefined) {
      consumerRunConfig = third;
    }

    if (subscription === undefined) {
      throw new Error("Unable to handle for subscription");
    }

    let { consumer } = this;
    if (consumer === undefined) {
      if (config === undefined) {
        throw new Error("Please use overload or setupConsumer function");
      }

      consumer = this.kafka.consumer(config);
    }

    await consumer.connect();
    await consumer.subscribe(subscription);
    await consumer.run(consumerRunConfig);
  }
}

export default PublishSubscribeService;
