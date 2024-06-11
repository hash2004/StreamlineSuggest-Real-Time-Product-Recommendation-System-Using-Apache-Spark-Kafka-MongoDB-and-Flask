from confluent_kafka import Producer

def delivery_report(err, msg):
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))

p = Producer({'bootstrap.servers': 'localhost:9092'})

@app.route('/result', methods=['GET','POST'])
def result():
    if request.method == 'POST':

        top_5_asin_list = [{"asin_index": row.asin_index} for row in top_5_asin]

        p.produce('test', value=json.dumps({"top_5_asin": top_5_asin_list}), callback=delivery_report)
        p.flush()

        return json.dumps({"top_5_asin": top_5_asin_list})

    else:
        return render_template('result.html')
