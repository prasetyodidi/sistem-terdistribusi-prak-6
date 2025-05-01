const readline = require('readline');
const { Kafka } = require('kafkajs');

const kafka = new Kafka({
    clientId: 'customer-app',
    brokers: [
        'localhost:9091',
        'localhost:9092',
        'localhost:9093',
    ]
});

const producer = kafka.producer();
const consumer = kafka.consumer({ groupId: 'customer-group' });

const rl = readline.createInterface({
    input: process.stdin,
    output: process.stdout,
    prompt: 'customer> ',
});

async function run() {
    await producer.connect();
    await consumer.connect();

    // Subscribe to the "konfirmasi_pesanan" topic to receive confirmation
    await consumer.subscribe({ topic: 'konfirmasi_pesanan', fromBeginning: true });

    consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
            try {
                const confirmation = JSON.parse(message.value.toString());
                console.log(`Confirmation received for Order ID: ${confirmation.id}`);
                console.log(`Status: ${confirmation.status}`);
                console.log(`Table Number: ${confirmation.nomorMeja}`);
                console.log(`Date: ${new Date(confirmation.tanggal * 1000).toLocaleString()}`);
            } catch (error) {
                console.error('Failed to process confirmation message:', error);
            }
        },
    });

    console.log('Welcome to the Customer CLI');
    console.log('Commands: order, exit');
    rl.prompt();

    rl.on('line', async (line) => {
        const [command, ...args] = line.trim().split(' ');
        switch (command) {
            case 'order':
                const order = {
                    id: `order-${Date.now()}`,
                    tanggal: Math.floor(Date.now() / 1000),
                    status: 'Diterima',
                    nomorMeja: Math.floor(Math.random() * 10) + 1, // Random table number
                    items: [],
                    totalHarga: 0,
                };

                console.log('Enter items for the order (type "done" when finished):');
                const addItem = () => {
                    rl.question('Item name: ', (nama) => {
                        if (nama.toLowerCase() === 'done') {
                            console.log('Order completed.');
                            console.log(order);

                            // Send the order to Kafka
                            producer.send({
                                topic: 'pemesan',
                                messages: [{ value: JSON.stringify(order) }],
                            }).then(() => {
                                console.log(`Order sent with ID: ${order.id}`);
                                rl.prompt();
                            }).catch(console.error);

                            return;
                        }

                        rl.question('Quantity: ', (jumlah) => {
                            rl.question('Price: ', (harga) => {
                                const item = {
                                    nama,
                                    jumlah: parseInt(jumlah, 10),
                                    harga: parseInt(harga, 10),
                                };
                                order.items.push(item);
                                order.totalHarga += item.jumlah * item.harga;

                                console.log(`Added item: ${JSON.stringify(item)}`);
                                addItem(); // Prompt for the next item
                            });
                        });
                    });
                };

                addItem();
                break;

            case 'exit':
                await producer.disconnect();
                await consumer.disconnect();
                rl.close();
                break;

            default:
                console.log('Unknown command. Try: order, exit');
        }
        rl.prompt();
    }).on('close', () => {
        console.log('Exiting Customer CLI. Goodbye!');
        process.exit(0);
    });
}

run().catch(console.error);