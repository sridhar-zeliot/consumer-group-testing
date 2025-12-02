const { Kafka } = require("kafkajs");

const kafka = new Kafka({
  clientId: process.env.CLIENT_ID,
  brokers: ['my-cluster-kafka-bootstrap.kafka.svc:9092'],
  sasl: {
    mechanism: "scram-sha-512",
    username: process.env.SASL_USERNAME,
    password: process.env.SASL_PASSWORD,
  },
  namespace: "kafka"
});

const consumer = kafka.consumer({
  groupId: process.env.CONSUMER_GROUP_ID,
});

const run = async () => {
  await consumer.connect();
// This single line connects publisher to consumer through Kafka
// await consumer.subscribe({ topics: process.env.KAFKA_TOPICS.split(","), fromBeginning: true });

  // // Subscribe to both topics
  // await consumer.subscribe({
  //   topics: ["commontopic", "commoninputtopic"],
  //   fromBeginning: true,
  // });

  const topics = process.env.KAFKA_TOPICS
  .split(",")
  .map(t => t.trim());

 await consumer.subscribe({ topics, fromBeginning: true });


  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      console.log(`📥 Consumed from ${topic} [${partition}]`);
      console.log("Key:", message.key?.toString());
      console.log("Value (raw):", message.value.toString());
      console.log("Headers:", message.headers);
      console.log("--------------------------------------");
    },
  });
};

run().catch(console.error);
