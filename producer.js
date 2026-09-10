
const net = require('node:net');
const { Kafka, Partitioners } = require('kafkajs');
const avro = require('avsc');
const fs = require('node:fs');
const path = require('node:path');
const { randomUUID } = require('node:crypto');
const { setTimeout: delay } = require('node:timers/promises');

// Load the schema and prepare the Avro encoder.
const schema = JSON.parse(
  fs.readFileSync(path.join(__dirname, 'order.avsc'), 'utf8')
);
const orderType = avro.Type.forSchema(schema);

// Connect to Kafka running in Docker.
const kafka = new Kafka({
  clientId: 'order-producer',
  brokers: ['127.0.0.1:9092'],

  socketFactory: ({ host, port, onConnect }) => {
    const socket = net.connect(
      {
        host,
        port,
        family: 4,
      },
      onConnect
    );

    socket.setKeepAlive(true, 30000);
    return socket;
  },
});

const producer = kafka.producer({
  createPartitioner: Partitioners.DefaultPartitioner,
});

async function main() {
  try {
    await producer.connect();
    console.log('Connected to Kafka.');

    for (let i = 1; i <= 10; i++) {
      const order = {
        orderId: randomUUID(),
        product: `Item${i}`,
        price: i === 5
            ? -50
            : Number((10 + Math.random() * 490).toFixed(2)),
      };

      // Convert the order into Avro binary data.
      const encodedOrder = orderType.toBuffer(order);

      // Wait for Kafka to acknowledge the message.
      await producer.send({
        topic: 'orders',
        messages: [
          {
            key: order.orderId,
            value: encodedOrder,
          },
        ],
      });

      console.log(`Sent order ${i}:`, order);

      if (i < 10) {
        await delay(1000);
      }
    }

    console.log('Finished sending 10 orders.');
  } finally {
    await producer.disconnect();
  }
}

main().catch((error) => {
  console.error('Producer failed:', error);
  process.exitCode = 1;
});