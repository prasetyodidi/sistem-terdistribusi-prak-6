# Praktikum ke 6 mata kuliah Sistem Terdistribusi

### order status
- Diterima: Order masuk, menunggu konfirmasi.
- Dikonfirmasi: Order diteruskan ke Dapur.
- Diproses: Dapur sedang memasak.
- Siap: Makanan siap diantar.
- Selesai: Makanan di meja Customer.
- Dibayar: Pembayaran diterima.


docker exec -it kafka1 /bin/bash

kafka-topics --delete --topic order --bootstrap-server localhost:9091

kafka-topics --create --topic order --partitions 3 --replication-factor 1 --bootstrap-server localhost:9091

kafka-topics --describe --topic order --bootstrap-server kafka1:9091

zookeeper-shell zoo1:2181 ls /brokers/ids
zookeeper-shell zoo1:2181 ls /brokers/topics


kafka-topics --create --topic test1 --bootstrap-server kafka1:9091

kafka-consumer-groups --bootstrap-server {bootstrap_server} --list