const readline = require('readline');
const { Kafka } = require('kafkajs');

const kafka = new Kafka({
    clientId: 'cashier-app',
    brokers: [
        'localhost:9091',
        'localhost:9092',
        'localhost:9093',
    ]
});

const producer = kafka.producer();
const consumer = kafka.consumer({ groupId: 'cashier-group' });

const rl = readline.createInterface({
    input: process.stdin,
    output: process.stdout,
    prompt: 'kasir> ',
});

async function run() {
    await producer.connect();
    await consumer.connect();

    // Subscribe to the "notifikasi" topic to receive notifications from the kitchen
    await consumer.subscribe({ topic: 'notifikasi', fromBeginning: true });

    consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
            try {
                const notification = JSON.parse(message.value.toString());
                console.log(`Notification received for Order ID: ${notification.id}`);
                console.log(`Status: ${notification.status}`);
                console.log(`Table Number: ${notification.nomorMeja}`);
                console.log(`Date: ${new Date(notification.tanggal * 1000).toLocaleString()}`);
                console.log('Items:');
                notification.items.forEach((item, index) => {
                    console.log(`${index + 1}. ${item.nama} - ${item.jumlah} pcs @ ${item.harga}`);
                });
                console.log(`Total Price: ${notification.totalHarga}`);

                // Prompt the cashier to process payment
                rl.question('Enter payment method (e.g., cash, card): ', async (paymentMethod) => {
                    const paymentDetails = {
                        id: notification.id,
                        nomorMeja: notification.nomorMeja,
                        totalHarga: notification.totalHarga,
                        metodePembayaran: paymentMethod,
                        tanggal: Math.floor(Date.now() / 1000),
                    };

                    // Send payment details to the "pembayaran" topic
                    await producer.send({
                        topic: 'pembayaran',
                        messages: [{ value: JSON.stringify(paymentDetails) }],
                    });

                    console.log(`Payment processed for Order ID: ${notification.id}`);
                    console.log(`Payment Method: ${paymentMethod}`);
                    console.log('Payment confirmation sent to the customer.');
                });
            } catch (error) {
                console.error('Failed to process notification message:', error);
            }
        },
    });

    console.log('Welcome to the Cashier CLI (kasir)');
    console.log('Listening for notifications from the kitchen...');
    console.log('Type "exit" to close the application.');
    rl.prompt();

    rl.on('line', async (line) => {
        const command = line.trim();
        if (command === 'exit') {
            await producer.disconnect();
            await consumer.disconnect();
            rl.close();
        } else {
            console.log('Unknown command. Type "exit" to close the application.');
        }
        rl.prompt();
    }).on('close', () => {
        console.log('Exiting Cashier CLI. Goodbye!');
        process.exit(0);
    });
}

run().catch(console.error);