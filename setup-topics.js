const { createKafka } = require('./kafka-client');

const TOPICS = ['orders', 'orders-dlq'];

async function main() {
  const kafka = createKafka('setup-topics');
  const admin = kafka.admin();

  await admin.connect();

  try {
    const existingTopics = await admin.listTopics();
    const topicsToCreate = TOPICS.filter((t) => !existingTopics.includes(t));

    if (topicsToCreate.length === 0) {
      console.log('All required topics already exist:', TOPICS.join(', '));
      return;
    }

    const created = await admin.createTopics({
      topics: topicsToCreate.map((topic) => ({
        topic,
        numPartitions: 1,
        replicationFactor: 1,
      })),
    });

    if (created) {
      console.log('Created topics:', topicsToCreate.join(', '));
    } else {
      console.log('Topics already existed by the time creation was attempted.');
    }

    const stillMissing = TOPICS.filter((t) => !topicsToCreate.includes(t));
    if (stillMissing.length > 0) {
      console.log('Already present:', stillMissing.join(', '));
    }
  } finally {
    await admin.disconnect();
  }
}

main().catch((err) => {
  console.error('Failed to set up topics:', err);
  process.exitCode = 1;
});
