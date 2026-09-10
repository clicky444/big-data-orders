const { Kafka } = require('kafkajs');
const avro = require('avsc');
const fs = require('node:fs');
const path = require('node:path');
const net = require('node:net');

const schema = JSON.parse(
  fs.readFileSync(path.join(__dirname, 'order.avsc'), 'utf8')
);

const orderType = avro.Type.forSchema(schema);

function createKafka(clientId) {
  return new Kafka({
    clientId,
    brokers: ['127.0.0.1:9092'],

    socketFactory: ({ host, port, onConnect }) => {
      const socket = net.connect(
        { host, port, family: 4 },
        onConnect
      );

      socket.setKeepAlive(true, 30000);
      return socket;
    },
  });
}

module.exports = { createKafka, orderType };