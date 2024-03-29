from flask import Flask, render_template, request, jsonify
from kafka import KafkaConsumer, KafkaProducer
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, DoubleType
from pyspark.sql.functions import lit
from pyspark.ml import PipelineModel
from pyspark.sql.functions import desc
import json

app = Flask(__name__)

# Initialize Spark
spark = SparkSession \
    .builder \
    .appName("FT") \
    .master("local") \
    .config("spark.mongodb.input.uri", "mongodb://localhost:27017/") \
    .config("spark.mongodb.output.uri", "mongodb://localhost:27017/") \
    .config("spark.driver.memory", "12g") \
    .config("spark.executor.memory", "12g") \
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1") \
    .getOrCreate()

# Initialize Kafka producer
producer = KafkaProducer(bootstrap_servers='localhost:9092')

# Initialize Kafka consumer
consumer = KafkaConsumer(
    'test',
    bootstrap_servers='localhost:9092',
    group_id='mygroup',
    auto_offset_reset='earliest'
)
consumer.subscribe(['test'])

# Function to process the received message
def process_message(message):
    # Extract the message value 
    value = message.value.decode('utf-8')

    # Do something with the message value
    print("Received message:", value)

# Function to send Kafka messages
def send_message(message):
    producer.send('test', message.encode('utf-8'))
    producer.flush()

# Route for home page
@app.route('/')
def home():
    return render_template('index.html')

# Route for registration
@app.route('/register')
def register():
    return render_template('register.html')

# Route for processing the form and sending Kafka message
@app.route('/result', methods=['GET', 'POST'])
def result():
    if request.method == 'POST':
        reviewer_id = request.form.get('reviewer-id')

        # Load the saved model
        path = "file:///home/dante/Desktop/Big_data/model_mf"
        loaded_model_mf = PipelineModel.load(path)

        csv_path = "file:///home/dante/Desktop/Big_data/id.csv"
        schema = StructType([StructField("asin", DoubleType(), nullable=True)])
        df = spark.read.csv(csv_path, header=True, schema=schema)

        value = float(reviewer_id)
        final_df = df.withColumn("reviewer_id", lit(value))
        df = final_df.select("reviewer_id", "asin")
        final_df = df.withColumnRenamed("reviewer_id", "reviewerID_index").withColumnRenamed("asin", "asin_index")

        user_predictions = loaded_model_mf.transform(final_df)
        sorted_predictions = user_predictions.orderBy(desc("prediction"))
        top_5_asin = sorted_predictions.select("asin_index").limit(5).collect()

        top_5_asin_list = [{"asin_index": row.asin_index} for row in top_5_asin]

        # Send the top 5 ASINs to Kafka
        send_message(json.dumps({"top_5_asin": top_5_asin_list}))

        return jsonify({"top_5_asin": top_5_asin_list})

    else:
        return render_template('result.html')

# Function to consume Kafka messages
def consume_messages():
    for message in consumer:
        process_message(message)

# Function to process the received message
def process_message(message):
    # Extract the message value
    value = message.value.decode('utf-8')

    # Do something with the message value
    print("Received message:", value)

# Run the Kafka consumer in a separate thread
import threading

def start_consumer():
    threading.Thread(target=consume_messages).start()

# Start the Kafka consumer in a separate thread
start_consumer()

if __name__ == '__main__':
    app.run()
