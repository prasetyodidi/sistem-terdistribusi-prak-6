import { Kafka } from 'kafkajs';

const kafka = new Kafka({
    clientId: 'didi-consumer',
    brokers: [
        // '192.168.8.113:9092',

        'localhost:9091',
        'localhost:9092',
        'localhost:9093',
    ]
});

const consumer = kafka.consumer({ groupId: 'kitchen' });

const run = async () => {
    await consumer.connect();
    console.log('Consumer connected');

    await consumer.subscribe({ topic: 'notifikasi', fromBeginning: true });

    await consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
            console.log(`Pesan diterima: ${message.value.toString()}`);
        },
    });
};

run().catch(console.error);