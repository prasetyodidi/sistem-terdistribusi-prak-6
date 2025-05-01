const { Kafka } = require('kafkajs');

const kafka = new Kafka({
    clientId: 'waiter-app',
    brokers: [
        'localhost:9091',
        'localhost:9092',
        'localhost:9093',
    ]
});

const consumer = kafka.consumer({ groupId: 'waiter-group' });
const producer = kafka.producer();

async function run() {
    await producer.connect();
    await consumer.connect();

    // Subscribe to the "pemesan" topic
    await consumer.subscribe({ topic: 'pemesan', fromBeginning: true });

    console.log('Waiter service is running...');
    console.log('Listening for orders from the "pemesan" topic...');

    consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
            try {
                const orderData = JSON.parse(message.value.toString());
                console.log(`Received new order from topic "${topic}":`);
                console.log(`Order ID: ${orderData.id}`);
                console.log(`Date: ${new Date(orderData.tanggal * 1000).toLocaleString()}`);
                console.log(`Status: ${orderData.status}`);
                console.log(`Table Number: ${orderData.nomorMeja}`);
                console.log('Items:');
                orderData.items.forEach((item, index) => {
                    console.log(`${index + 1}. ${item.nama} - ${item.jumlah} pcs @ ${item.harga}`);
                });
                console.log(`Total Price: ${orderData.totalHarga}`);

                // Send confirmation to the "konfirmasi_pesanan" topic
                const confirmationMessage = {
                    id: orderData.id,
                    status: 'Dikonfirmasi',
                    tanggal: orderData.tanggal,
                    nomorMeja: orderData.nomorMeja,
                };

                await producer.send({
                    topic: 'konfirmasi_pesanan',
                    messages: [{ value: JSON.stringify(confirmationMessage) }],
                });

                console.log(`Order confirmation sent for Order ID: ${orderData.id}`);

                // Forward the order details to the "order" topic
                await producer.send({
                    topic: 'order',
                    messages: [{ value: JSON.stringify(orderData) }],
                });

                console.log(`Order forwarded to the kitchen for Order ID: ${orderData.id}`);
            } catch (error) {
                console.error('Failed to process message:', error);
            }
        },
    });
}

run().catch(console.error);