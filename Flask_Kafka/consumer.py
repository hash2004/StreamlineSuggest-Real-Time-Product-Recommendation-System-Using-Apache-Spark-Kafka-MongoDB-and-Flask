from confluent_kafka import Consumer, KafkaError

c = Consumer({
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'mygroup',
    'auto.offset.reset': 'earliest'
})

c.subscribe(['test'])

@app.route('/stream')
def stream():
    def event_stream():
        while True:
            msg = c.poll(1.0)

            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(msg.error())
                    break

            yield 'data: {}\n\n'.format(msg.value().decode('utf-8'))

    return Response(event_stream(), mimetype="text/event-stream")
