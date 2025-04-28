const readline = require('readline');
const EventEmitter = require('events');

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

const kitchen = new Kitchen();

kitchen.on('orderCompleted', (order) => {
    console.log(`Notification: The order "${order}" is ready!`);
});

const rl = readline.createInterface({
    input: process.stdin,
    output: process.stdout,
    prompt: 'koki> ',
});

console.log('Welcome to the Kitchen CLI (koki)');
console.log('Commands: add [order], list, complete, exit');
rl.prompt();

rl.on('line', (line) => {
    const [command, ...args] = line.trim().split(' ');
    switch (command) {
        case 'add':
            const order = args.join(' ');
            if (order) {
                kitchen.addOrder(order);
            } else {
                console.log('Please specify an order to add.');
            }
            break;
        case 'list':
            kitchen.showOrders();
            break;
        case 'complete':
            kitchen.completeOrder();
            break;
        case 'exit':
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