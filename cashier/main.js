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

const orders = []; // Store received orders

async function run() {
    await producer.connect();
    await consumer.connect();

    // Subscribe to the "notifikasi" topic to receive notifications from the kitchen
    await consumer.subscribe({ topic: 'notifikasi', fromBeginning: true });

    consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
            try {
                const notification = JSON.parse(message.value.toString());
                orders.push(notification); // Add the order to the list
                console.log(`New order received: Order ID ${notification.id}`);
            } catch (error) {
                console.error('Failed to process notification message:', error);
            }
        },
    });

    console.log('Welcome to the Cashier CLI (kasir)');
    console.log('Listening for notifications from the kitchen...');
    console.log('Type "list" to view orders, "pay <order_id>" to process payment, or "exit" to close the application.');
    rl.prompt();

    rl.on('line', async (line) => {
        const command = line.trim();
        if (command === 'exit') {
            await producer.disconnect();
            await consumer.disconnect();
            rl.close();
        } else if (command === 'list') {
            if (orders.length === 0) {
                console.log('No orders available.');
            } else {
                console.log('Orders:');
                orders.forEach((order, index) => {
                    console.log(`${index + 1}. Order ID: ${order.id}, Table: ${order.nomorMeja}, Total: ${order.totalHarga}`);
                });
            }
        } else if (command.startsWith('pay ')) {
            const orderId = command.split(' ')[1];
            const order = orders.find(o => o.id === orderId);

            if (!order) {
                console.log(`Order with ID ${orderId} not found.`);
            } else {
                console.log(`Processing payment for Order ID: ${order.id}`);
                console.log(`Table Number: ${order.nomorMeja}`);
                console.log(`Total Price: ${order.totalHarga}`);
                console.log('Items:');
                order.items.forEach((item, index) => {
                    console.log(`${index + 1}. ${item.nama} - ${item.jumlah} pcs @ ${item.harga}`);
                });

                rl.question('Enter payment method (e.g., cash, card): ', async (paymentMethod) => {
                    const paymentDetails = {
                        id: order.id,
                        nomorMeja: order.nomorMeja,
                        totalHarga: order.totalHarga,
                        metodePembayaran: paymentMethod,
                        tanggal: Math.floor(Date.now() / 1000),
                    };

                    // Send payment details to the "pembayaran" topic
                    await producer.send({
                        topic: 'pembayaran',
                        messages: [{ value: JSON.stringify(paymentDetails) }],
                    });

                    console.log(`Payment processed for Order ID: ${order.id}`);
                    console.log(`Payment Method: ${paymentMethod}`);
                    console.log('Payment confirmation sent to the customer.');

                    // Remove the order from the list after payment
                    const index = orders.indexOf(order);
                    if (index > -1) {
                        orders.splice(index, 1);
                    }
                });
            }
        } else {
            console.log('Unknown command. Type "list" to view orders, "pay <order_id>" to process payment, or "exit" to close the application.');
        }
        rl.prompt();
    }).on('close', () => {
        console.log('Exiting Cashier CLI. Goodbye!');
        process.exit(0);
    });
}

run().catch(console.error);