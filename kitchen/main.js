const readline = require('readline');
const EventEmitter = require('events');
const { Kafka } = require('kafkajs');

class Kitchen extends EventEmitter {
    constructor() {
        super();
        this.orders = [];
    }

    addOrder(order) {
        this.orders.push(order);
        console.log(`Order added: ${order.id}`);
    }

    showOrders() {
        if (this.orders.length === 0) {
            console.log('No orders available.');
        } else {
            console.log('Current orders:');
            this.orders.forEach((order, index) => {
                console.log(`${index + 1}. ${order.id}`);
            });
        }
    }

    completeOrder() {
        if (this.orders.length === 0) {
            console.log('No orders to complete.');
        } else {
            const completedOrder = this.orders.shift();
            console.log(`Order completed: ${completedOrder.id}`);
            
            // Modify the order status to "siap"
            const updatedOrder = { ...completedOrder, status: 'siap' };

            // Emit the updated order to the notification topic
            this.emit('orderCompleted', updatedOrder);
        }
    }
}

const kafka = new Kafka({
    clientId: 'kitchen-app',
    brokers: [
        'localhost:9091',
        'localhost:9092',
        'localhost:9093',
    ]
});

const producer = kafka.producer();
const consumer = kafka.consumer({ groupId: 'kitchen-group' });

const kitchen = new Kitchen();

kitchen.on('orderCompleted', async (order) => {
    console.log(`Notification: The order "${order.id}" is ready!`);
    await producer.send({
        topic: 'notifikasi',
        messages: [{ value: JSON.stringify(order) }],
    });
});

const rl = readline.createInterface({
    input: process.stdin,
    output: process.stdout,
    prompt: 'koki> ',
});

async function run() {
    await producer.connect();
    await consumer.connect();

    await consumer.subscribe({ topic: 'order', fromBeginning: true });

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
    
                // Add the order ID to the kitchen's orders list
                kitchen.addOrder(orderData);
            } catch (error) {
                console.error('Failed to process message:', error);
            }
        },
    });

    console.log('Welcome to the Kitchen CLI (koki)');
    console.log('Commands: add [order], list, complete, exit');
    rl.prompt();

    rl.on('line', async (line) => {
        const [command, ...args] = line.trim().split(' ');
        switch (command) {
            case 'list':
                kitchen.showOrders();
                break;
            case 'complete':
                kitchen.completeOrder();
                break;
            case 'exit':
                await producer.disconnect();
                await consumer.disconnect();
                rl.close();
                break;
            default:
                console.log('Unknown command. Try: add [order], list, complete, exit');
        }
        rl.prompt();
    }).on('close', () => {
        console.log('Exiting Kitchen CLI. Goodbye!');
        process.exit(0);
    });
}

run().catch(console.error);