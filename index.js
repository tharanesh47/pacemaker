import pkg from "kafkajs";
const { Kafka } = pkg;
import dotenv from "dotenv";
dotenv.config();

const kafka = new Kafka({
  clientId: "pacemaker-data-" + Date.now(), // Append Current Epoch milliseconds for Random Id
  brokers: [
    process.env.KAFKA_BOOTSTRAP_SERVER_URL ||
      "my-cluster-kafka-bootstrap.kafka:9092",
  ],
  sasl: {
    mechanism: "scram-sha-512",
    username: process.env.KAFKA_USERNAME,
    password: process.env.KAFKA_PASSWORD,
  },
});

const producer = kafka.producer();

const produceMessage = async (topic, message) => {
  console.log("Producing Message: ", message);
  await producer.connect();
  await producer.send({
    topic,
    messages: [
      {
        value: message,
      },
    ],
  });
  await producer.disconnect();
};

let patientIds = ["202028261", "202028262", "202028263", "202028264"];

const pacemakerModels = [
  "Medtronic Adapta",
  "Boston Scientific Ingenio",
  "St. Jude Medical Assurity",
  "Biotronik Evia",
  "Sorin Reply",
];

async function generatePacemakerData() {
  let index = Math.floor(Math.random() * 3);
  let id = patientIds[index];
  const paceMakerData = {
    id: `${id}`,
    name: `Patient ${id}`,
    age: Math.floor(Math.random() * 40) + 50,
    pacemaker_model:
      pacemakerModels[Math.floor(Math.random() * pacemakerModels.length)],
    battery_status: `${(Math.random() * 100).toFixed(2)}%`,
    heart_rate: Math.floor(Math.random() * 40) + 60,
    last_checkup: new Date(
      Date.now() - Math.random() * 31536000000
    ).toISOString(),
  };

  return paceMakerData;
}

setInterval(async () => {
  const message = await generatePacemakerData();
//   console.log(message);
  produceMessage(process.env.PUBLISH_TOPIC, JSON.stringify(message));
}, 5000);
