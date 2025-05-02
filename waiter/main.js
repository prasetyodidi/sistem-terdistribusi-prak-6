const { Kafka } = require('kafkajs');
const readline = require('readline');

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

const rl = readline.createInterface({
    input: process.stdin,
    output: process.stdout,
});

let pendingOrders = [];

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

                // Add the order to the pending orders list
                pendingOrders.push(orderData);
                console.log(`Order ID: ${orderData.id} added to pending orders.`);
            } catch (error) {
                console.error('Failed to process message:', error);
            }
        },
    });

    // Waiter confirmation loop
    handleOrderConfirmation();
}

function listPendingOrders() {
    if (pendingOrders.length === 0) {
        console.log('\nNo pending orders at the moment.');
    } else {
        console.log('\nPending Orders:');
        pendingOrders.forEach((order, index) => {
            console.log(`${index + 1}. Order ID: ${order.id}, Table Number: ${order.nomorMeja}`);
        });
    }
}

function handleOrderConfirmation() {
    console.log('\nOptions:');
    console.log('1. List pending orders');
    console.log('2. Confirm an order');
    console.log('3. Exit');
    rl.question('Choose an option (1, 2, or 3): ', async (input) => {
        if (input === '1') {
            listPendingOrders();
            handleOrderConfirmation(); // Return to the options menu
        } else if (input === '2') {
            if (pendingOrders.length === 0) {
                console.log('No pending orders to confirm.');
                handleOrderConfirmation();
                return;
            }

            console.log('\nPending Orders:');
            pendingOrders.forEach((order, index) => {
                console.log(`${index + 1}. Order ID: ${order.id}, Table Number: ${order.nomorMeja}`);
            });

            rl.question('Enter the number of the order to confirm: ', async (orderInput) => {
                const orderIndex = parseInt(orderInput) - 1;

                if (orderIndex >= 0 && orderIndex < pendingOrders.length) {
                    const orderData = pendingOrders[orderIndex];

                    // Send confirmation to the "konfirmasi_pesanan" topic
                    const confirmationMessage = {
                        id: orderData.id,
                        status: 'Dikonfirmasi',
                        tanggal: orderData.tanggal,
                        nomorMeja: orderData.nomorMeja,
                    };

                    try {
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

                        // Remove the confirmed order from the pending list
                        pendingOrders.splice(orderIndex, 1);
                    } catch (error) {
                        console.error('Failed to confirm order:', error);
                    }
                } else {
                    console.log('Invalid selection. Please try again.');
                }

                handleOrderConfirmation(); // Continue the confirmation loop
            });
        } else if (input === '3') {
            console.log('Exiting the application. Goodbye!');
            rl.close();
            process.exit(0);
        } else {
            console.log('Invalid option. Please try again.');
            handleOrderConfirmation();
        }
    });
}

run().catch(console.error);