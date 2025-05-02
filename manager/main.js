const { Kafka } = require('kafkajs');

const kafka = new Kafka({
    clientId: 'manager-app',
    brokers: [
        'localhost:9091',
        'localhost:9092',
        'localhost:9093',
    ]
});

const consumer = kafka.consumer({ groupId: 'manager-group' });

const orders = new Map(); // Store orders with their timestamps for analysis

async function run() {
    await consumer.connect();

    // Subscribe to all relevant topics
    await consumer.subscribe({ topic: 'pemesan', fromBeginning: true });
    await consumer.subscribe({ topic: 'konfirmasi_pesanan', fromBeginning: true });
    await consumer.subscribe({ topic: 'order', fromBeginning: true });
    await consumer.subscribe({ topic: 'notifikasi', fromBeginning: true });
    await consumer.subscribe({ topic: 'pembayaran', fromBeginning: true });

    console.log('Manager service is running...');
    console.log('Monitoring all topics for performance analysis...');

    consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
            try {
                const data = JSON.parse(message.value.toString());
                const timestamp = Date.now();

                switch (topic) {
                    case 'pemesan':
                        console.log(`[${new Date(timestamp).toLocaleString()}] New order received: ${data.id}`);
                        orders.set(data.id, { ...data, timestamps: { pemesan: timestamp } });
                        break;

                    case 'konfirmasi_pesanan':
                        console.log(`[${new Date(timestamp).toLocaleString()}] Order confirmed: ${data.id}`);
                        if (orders.has(data.id)) {
                            orders.get(data.id).timestamps.konfirmasi_pesanan = timestamp;
                        }
                        break;

                    case 'order':
                        console.log(`[${new Date(timestamp).toLocaleString()}] Order sent to kitchen: ${data.id}`);
                        if (orders.has(data.id)) {
                            orders.get(data.id).timestamps.order = timestamp;
                        }
                        break;

                    case 'notifikasi':
                        console.log(`[${new Date(timestamp).toLocaleString()}] Order ready: ${data.id}`);
                        if (orders.has(data.id)) {
                            orders.get(data.id).timestamps.notifikasi = timestamp;
                        }
                        break;

                    case 'pembayaran':
                        console.log(`[${new Date(timestamp).toLocaleString()}] Payment processed: ${data.id}`);
                        if (orders.has(data.id)) {
                            orders.get(data.id).timestamps.pembayaran = timestamp;
                        }
                        break;

                    default:
                        console.log(`[${new Date(timestamp).toLocaleString()}] Unknown topic: ${topic}`);
                }
            } catch (error) {
                console.error('Failed to process message:', error);
            }
        },
    });

    // Generate monthly performance report
    setInterval(() => {
        console.log('\n=== Monthly Performance Report ===');
        const now = Date.now();
        const oneMonthAgo = now - 30 * 24 * 60 * 60 * 1000;

        const completedOrders = Array.from(orders.values()).filter(order => 
            order.timestamps.pembayaran && order.timestamps.pembayaran >= oneMonthAgo
        );

        console.log(`Total completed orders: ${completedOrders.length}`);

        let totalProcessingTime = 0;
        completedOrders.forEach(order => {
            const processingTime = order.timestamps.pembayaran - order.timestamps.pemesan;
            totalProcessingTime += processingTime;
            console.log(`Order ID: ${order.id}, Processing Time: ${processingTime / 1000} seconds`);
        });

        if (completedOrders.length > 0) {
            const averageProcessingTime = totalProcessingTime / completedOrders.length;
            console.log(`Average Processing Time: ${(averageProcessingTime / 1000).toFixed(2)} seconds`);
        } else {
            console.log('No completed orders in the last month.');
        }

        console.log('=================================\n');
    }, 30 * 1000); // Generate report every 30 seconds for demonstration purposes
}

run().catch(console.error);