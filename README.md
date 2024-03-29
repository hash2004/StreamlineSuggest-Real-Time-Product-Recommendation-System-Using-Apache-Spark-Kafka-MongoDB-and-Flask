# StreamlineSuggest-Real-Time-Product-Recommendation-System-Using-Apache-Spark-Kafka-MongoDB-and-Flask
# # Introduction
The project aims to build a live product recommender system based on customer preferences using the [Amazon product dataset]([url](https://nijianmo.github.io/amazon/index.html)). By leveraging big data processing, NoSQL databases, machine learning algorithms, web development, and stream processing, the project seeks to provide users with personalized recommendations, enhancing their shopping experience on an e-commerce platform.

# # Background and Purpose
In today's digital era, e-commerce platforms have become a significant part of our daily lives,
offering a vast array of products and services. However, with the abundance of options, users
often face difficulties in discovering products that align with their interests and preferences. This
challenge highlights the need for an intelligent recommender system that can analyze user data
and provide tailored recommendations.
The purpose of this project is to address this need by developing a product recommender system
using the Amazon product dataset. By utilizing advanced technologies such as Apache Spark for
data processing, MongoDB for efficient data storage, machine learning algorithms for model
training, Flask for web application development, and Apache Kafka for real-time streaming, the
project aims to deliver accurate and timely recommendations to users.
Overall, this project offers a comprehensive opportunity to gain hands-on experience in various
domains, including big data processing, NoSQL databases, machine learning, web development,
and stream processing.

# # Data Loading
To optimize the data loading process for a future sampling requirement, I employed a well-defined schema. Recognizing the potential size of the data (123 GB), I took the initiative to incorporate a partition key during the initial load into MongoDB. This strategic decision will significantly improve efficiency when sampling the data for visualizations and analysis, saving valuable time and resources down the line.

In the code I used, the partitionkey option is used to specify the partition key for writing the
DataFrame to MongoDB. The partition key plays a significant role in the distribution and
organization of data within MongoDB. By defining the partition key, you determine how the data
will be partitioned and distributed across different nodes or shards in the MongoDB cluster.
The partition key is used to group and store related data together based on a specific field or
attribute. In this case, the asin field is chosen as the partition key. By using the asin field as the
partition key, the data will be partitioned and stored based on the unique values of the asin field.
This means that all records with the same asin value will be stored together within the same
partition or shard.

The choice of partition key can have a significant impact on query performance, data
distribution, and scalability. In the context of the provided code, using asin as the partition key
can facilitate sampling and querying of the data. Since records with the same asin value are
stored together, it becomes easier to sample a specific product's reviews or retrieve all reviews
for a particular product. This can improve the efficiency of subsequent analysis or model training
processes that require accessing specific subsets of the data based on the asin field.
By leveraging the partition key and the underlying partitioning mechanism in MongoDB, the
code optimizes data organization and access patterns. It enables efficient data retrieval, sampling,
and subsequent processing by leveraging the partitioning strategy based on the chosen partition
key.

# # Sampling
Proof of sampling accuracy: 


