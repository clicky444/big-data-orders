const { Partitioners } = require('kafkajs');
const { setTimeout: delay } = require('node:timers/promises');
const { createKafka, orderType } = require('./kafka-client');

const kafka = createKafka('order-consumer');

const consumer = kafka.consumer({
  groupId: 'order-average-group',
});

const dlqProducer = kafka.producer({
  createPartitioner: Partitioners.DefaultPartitioner,
});

const MAX_ATTEMPTS = 3;
const RETRY_DELAY_MS = 1000;

let totalPrice = 0;
let orderCount = 0;

// Validate the order and simulate processing failures.
async function processOrder(order, attempt) {
  if (!Number.isFinite(order.price) || order.price < 0) {
    const error = new Error('Price must be finite and non-negative');
    error.temporary = false;
    throw error;
  }

  if (
    (order.product === 'Item3' && attempt < 3) ||
    order.product === 'Item7'
  ) {
    const error = new Error('Simulated service temporarily unavailable');
    error.temporary = true;
    throw error;
  }
}

async function processWithRetry(order, heartbeat) {
  for (let attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
    try {
      await processOrder(order, attempt);

      console.log(
        `[SUCCESS] ${order.product} processed on attempt ${attempt}`
      );

      return { success: true };
    } catch (error) {
      // Unexpected programming errors should not be treated as bad orders.
      if (typeof error.temporary !== 'boolean') {
        throw error;
      }

      console.log(
        `[FAILED] ${order.product} | ` +
        `Attempt ${attempt}/${MAX_ATTEMPTS} | ${error.message}`
      );

      if (!error.temporary || attempt === MAX_ATTEMPTS) {
        return {
          success: false,
          reason: error.message,
          attempts: attempt,
          failureType: error.temporary
            ? 'retries-exhausted'
            : 'permanent',
        };
      }

      console.log(`[RETRY] Waiting ${RETRY_DELAY_MS}ms...`);
    }

    await delay(RETRY_DELAY_MS);
    await heartbeat();
  }
}

async function main() {
  await dlqProducer.connect();
  await consumer.connect();

  await consumer.subscribe({
    topic: 'orders',
    fromBeginning: true,
  });

  await consumer.run({
    eachMessage: async ({ topic, partition, message, heartbeat }) => {
      const order = orderType.fromBuffer(message.value);
      const result = await processWithRetry(order, heartbeat);

      if (!result.success) {
        // Wait for Kafka to acknowledge the DLQ message.
        // If sending fails, let the error propagate.
        await dlqProducer.send({
          topic: 'orders-dlq',
          messages: [
            {
              key: message.key,
              value: message.value,
              headers: {
                'failure-reason': result.reason,
                'failure-type': result.failureType,
                attempts: String(result.attempts),
                'source-topic': topic,
                'source-partition': String(partition),
                'source-offset': message.offset,
                'failed-at': new Date().toISOString(),
              },
            },
          ],
        });

        console.log(
          `[DLQ] ${order.product} sent to orders-dlq | ` +
          `${result.failureType} | Excluded from average`
        );

        return;
      }

      totalPrice += order.price;
      orderCount += 1;

      console.log(
        `Received ${order.product} | ` +
        `Price: ${order.price.toFixed(2)} | ` +
        `Count: ${orderCount} | ` +
        `Running average: ${(totalPrice / orderCount).toFixed(2)}`
      );
    },
  });

  console.log('Consumer running. Press Ctrl+C to stop.');
}

async function shutdown() {
  // Finish consumer processing before closing its DLQ producer.
  await consumer.disconnect();
  await dlqProducer.disconnect();
}

main().catch(async (error) => {
  console.error('Consumer failed:', error);
  process.exitCode = 1;
  await shutdown();
});

process.once('SIGINT', () => {
  console.log('\nStopping consumer...');
  shutdown().catch((error) => {
    console.error('Shutdown failed:', error);
    process.exitCode = 1;
  });
});