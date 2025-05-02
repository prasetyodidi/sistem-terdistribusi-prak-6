import { Kafka } from 'kafkajs';
import readline from 'readline';

const kafka = new Kafka({
    clientId: 'my-producer',
    brokers: [
        // '192.168.31.188:9094',
        // '192.168.31.184:9092',
        // '192.168.31.116:9091',
        // '192.168.31.26:9093',

        // '192.168.50.203:9091',
        // '192.168.8.113:9092',

        'localhost:9091',
        'localhost:9092',
        'localhost:9093',

    ]
});

const producer = kafka.producer();

const rl = readline.createInterface({
    input: process.stdin,
    output: process.stdout
});

const promptInput = () => {
    rl.question('Ketik pesan (atau "quit" untuk keluar): ', async (input) => {
        if (input.toLowerCase() === 'quit') {
            console.log('Producer dihentikan.');
            await producer.disconnect();
            rl.close();
            return;
        }

        try {
            const test = {
                "id": crypto.randomUUID(),
                "tanggal": 1714302000,
                "status": "Dikonfirmasi",
                "nomorMeja": Math.floor(Math.random() * 100) + 1,
                "items": [
                    {
                        "id": 1,
                        "nama": "Nasi Goreng",
                        "harga": 20000,
                        "jumlah": 2
                    },
                    {
                        "id": 2,
                        "nama": "Ayam Penyet",
                        "harga": 30000,
                        "jumlah": 2
                    }
                ],
                "totalHarga": 100000
            }
            
            await producer.send({
                topic: 'order',
                messages: [{ value: JSON.stringify(test) }]
            });
            console.log('Pesan dikirim:', input);
        } catch (error) {
            console.error('Gagal mengirim pesan:', error);
        }

        promptInput();
    });
};

const run = async () => {
    await producer.connect();
    console.log('Producer terhubung ke Kafka');
    promptInput();
};

run().catch(console.error);