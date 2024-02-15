import os from "node:os";
import {
  ConsumerConfig,
  ConsumerRunConfig,
  ConsumerSubscribeTopics,
  ProducerRecord,
} from "kafkajs";
// eslint-disable-next-line import/no-named-as-default
import PublishSubscribeService from "./index";

describe("Index", () => {
  let brokers: string[];
  let clientId: string;

  beforeEach(() => {
    brokers = ["localhost:9092"];
    clientId = os.hostname();
  });

  describe("constructor", () => {
    test("should not throw error", () => {
      expect(() => {
        // eslint-disable-next-line no-unused-vars, @typescript-eslint/no-unused-vars
        const service = new PublishSubscribeService(brokers, clientId);
      }).not.toThrow();
    });

    test("should not throw error when string", () => {
      expect(() => {
        // eslint-disable-next-line no-unused-vars, @typescript-eslint/no-unused-vars
        const service = new PublishSubscribeService(brokers[0], clientId);
      }).not.toThrow();
    });
  });

  describe("functions", () => {
    let service: PublishSubscribeService;
    let config: ConsumerConfig;
    beforeEach(() => {
      service = new PublishSubscribeService(brokers, clientId);
      config = { groupId: "test" };
    });

    describe("setupConsumer", () => {
      test("should return", () => {
        expect(service.setupConsumer(config)).toBeDefined();
      });
    });

    describe("setupProducer", () => {
      test("should return", () => {
        expect(service.setupProducer()).toBeDefined();
      });
    });

    describe("disconnect", () => {
      test("should resolve", () => {
        expect(service.disconnect()).resolves.toBeUndefined();
      });

      test("should resolve when consumer is set", () => {
        expect(service.setupConsumer(config)).toBeDefined();
        expect(service.disconnect()).resolves.toBeUndefined();
      });

      test("should resolve when producer is set", () => {
        expect(service.setupProducer()).toBeDefined();
        expect(service.disconnect()).resolves.toBeUndefined();
      });
    });

    describe("publish", () => {
      let record: ProducerRecord;
      beforeEach(() => {
        record = {
          topic: "test",
          messages: [{ value: JSON.stringify({ test: 1 }) }],
        };
      });

      test("should resolve", () => {
        expect(service.publish(record)).resolves.toBeDefined();
      });
    });

    describe("subscribe", () => {
      let subscription: ConsumerSubscribeTopics;
      let consumerRunConfig: ConsumerRunConfig;
      beforeEach(() => {
        subscription = { topics: ["test"] };
        consumerRunConfig = {
          eachMessage: jest.fn(),
        };
      });

      test("should resolve", () => {
        expect(
          service.subscribe(config, subscription, consumerRunConfig),
        ).resolves.toBeUndefined();
      });

      test("should resolve when setupConsumer is called first", () => {
        expect(service.setupConsumer(config)).toBeDefined();
        expect(
          service.subscribe(subscription, consumerRunConfig),
        ).resolves.toBeUndefined();
      });

      test("should reject when setupConsumer is not called", () => {
        expect(
          service.subscribe(subscription, consumerRunConfig),
        ).rejects.toHaveProperty(
          "message",
          "Please use overload or setupConsumer function",
        );
      });

      test("should reject when subscription is not correct", () => {
        expect(
          service.subscribe(
            config,
            consumerRunConfig as ConsumerSubscribeTopics,
            consumerRunConfig,
          ),
        ).rejects.toHaveProperty(
          "message",
          "Unable to handle for subscription",
        );
      });
    });
  });
});
