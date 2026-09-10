const { createKafka, orderType } = require('./kafka-client');

const kafka = createKafka('dlq-reader');

const consumer = kafka.consumer({
  groupId: 'dlq-inspector-group',
});

async function main() {
  await consumer.connect();

  await consumer.subscribe({
    topic: 'orders-dlq',
    fromBeginning: true,
  });

  await consumer.run({
    eachMessage: async ({ message }) => {
      const order = orderType.fromBuffer(message.value);
      const headers = message.headers || {};

      console.log('\n--- Failed order ---');
      console.log('Order ID:', order.orderId);
      console.log('Product:', order.product);
      console.log('Price:', order.price.toFixed(2));
      console.log('Reason:', headers['failure-reason']?.toString());
      console.log('Type:', headers['failure-type']?.toString());
      console.log('Attempts:', headers.attempts?.toString());
      console.log('Source offset:', headers['source-offset']?.toString());
    },
  });

  console.log('DLQ reader running. Press Ctrl+C to stop.');
}

main().catch(async (error) => {
  console.error('DLQ reader failed:', error);
  process.exitCode = 1;
  await consumer.disconnect();
});

process.once('SIGINT', () => {
  consumer.disconnect().catch((error) => {
    console.error('Shutdown failed:', error);
    process.exitCode = 1;
  });
});