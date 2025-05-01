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
        console.log(`Order added: ${order}`);
    }

    showOrders() {
        if (this.orders.length === 0) {
            console.log('No orders available.');
        } else {
            console.log('Current orders:');
            this.orders.forEach((order, index) => {
                console.log(`${index + 1}. ${order}`);
            });
        }
    }

    completeOrder() {
        if (this.orders.length === 0) {
            console.log('No orders to complete.');
        } else {
            const completedOrder = this.orders.shift();
            console.log(`Order completed: ${completedOrder}`);
            this.emit('orderCompleted', completedOrder);
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
    console.log(`Notification: The order "${order}" is ready!`);
    await producer.send({
        topic: 'notifikasi',
        messages: [{ value: `Order completed: ${order}` }],
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
            const order = message.value.toString();
            console.log(`Received new order from topic "${topic}": ${order}`);
            kitchen.addOrder(order);
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