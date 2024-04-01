# StreamlineSuggest-Real-Time-Product-Recommendation-System-Using-Apache-Spark-Kafka-MongoDB-and-Flask
## Introduction
The project aims to build a live product recommender system based on customer preferences using the [Amazon product dataset]([url](https://nijianmo.github.io/amazon/index.html)). By leveraging big data processing, NoSQL databases, machine learning algorithms, web development, and stream processing, the project seeks to provide users with personalized recommendations, enhancing their shopping experience on an e-commerce platform.

## Background and Purpose
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

## Data Loading
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

## Sampling
Proof of sampling accuracy:

![](/Flask_Kafka/static/images/samplegit.png "Proof of Accuracy"))

## Feature Engineering
Feature engineering is a critical step in ensuring the optimal performance of machine learning
models. In this project, special attention was given to feature engineering to enhance the quality
and effectiveness of the recommendation system. Several key techniques were employed,
including handling missing values and transforming categorical variables into numeric
representations.

By addressing missing values within the dataset, potential biases and inaccuracies stemming
from incomplete data were mitigated. Various approaches, such as imputation or deletion, could
have been utilized based on the nature of the missing values and the specific requirements of the
project.

Additionally, transforming categorical variables into numeric form was crucial to incorporate
them into machine learning models. This conversion enables algorithms to effectively process
and analyze categorical data, which would otherwise pose challenges due to their non-numeric
nature. Techniques like one-hot encoding or label encoding could have been applied to
accomplish this transformation, depending on the specific characteristics of the categorical
variables.

Furthermore, a vector feature was created to facilitate compatibility with Spark ML models.
While it is mentioned that the vector feature was not utilized later in the code, its creation
indicates a proactive approach toward preparing the data for potential model training or future
iterations of the recommendation system. The vector feature represents a necessary step in
organizing the data in a format suitable for machine learning algorithms and can significantly
impact the accuracy and performance of the models.

## Machine Learning
In this project, collaborative filtering, specifically the Alternating Least Squares (ALS)
algorithm, was employed as the machine learning technique for training the recommendation
model. Collaborative filtering is a widely used approach in recommendation systems that
leverages the preferences and behavior of similar users or items to make personalized
recommendations.
### What is ALS (Alternating Least Squares)?
ALS is a matrix factorization-based collaborative filtering algorithm that decomposes the
user-item interaction matrix into low-rank matrices. It assumes that users' preferences can be
represented by a few latent factors and learns these factors through an iterative optimization
process. By utilizing ALS, the project aimed to capture the underlying patterns and relationships
within the Amazon product dataset, enabling the model to make accurate and relevant
recommendations to users.
The ALS algorithm in Spark MLlib provides a scalable and efficient implementation of
collaborative filtering. It handles large datasets by leveraging distributed computing capabilities
and parallel processing. During the model training phase, the algorithm optimizes the factorized
matrices to minimize the reconstruction error between predicted and actual user-item
interactions. This iterative optimization process allows the model to learn the latent factors that
best capture users' preferences.

By utilizing collaborative filtering with ALS, the project leveraged the collective wisdom of
users to make recommendations. The model identifies similar users or items based on their
historical interactions and infers preferences based on the behavior of like-minded users or
similar items. This approach overcomes the limitations of explicit user feedback or item
attributes by capturing implicit preferences and uncovering hidden connections within the
dataset.

The ALS-based collaborative filtering approach not only enables the model to generate
personalized recommendations but also exhibits good scalability, making it suitable for
large-scale recommendation systems. By employing this algorithm, the project aimed to deliver
accurate, relevant, and personalized recommendations to enhance the user experience and
engagement on the e-commerce platform.

### Data Pipeline in Spark Workflow
When training out the ALS model, I also used a pipeline, which further streamlined my
approach. Basically, the pipeline is utilized to streamline the process of training the ALS
(Alternating Least Squares) model and making predictions on the testing set. A pipeline in Spark
ML is a sequence of stages that are executed in a specified order. Each stage represents a distinct
data transformation or machine learning operation.
The specific purpose of using a pipeline in this code is to encapsulate and automate the entire
workflow, from data preparation to model training and prediction generation. By defining a
pipeline, all the necessary transformations and operations can be organized into a coherent
sequence, ensuring consistency and reproducibility.

 ## Flask-Based Web Application
 In the development of my Flask-based web application, we aimed to enhance its functionality
and user experience by integrating JavaScript (JS) components, which contributed to the project's
overall score. One of the key features implemented in the application is the Login Page, where
users are required to provide a valid email address and password for authentication purposes.
This ensures secure access to the application's personalized features.

Additionally, we implemented a thoughtful strategy to handle cases where users enter a
reviewerID that is new to the prediction model. Recognizing that collaborative filtering models,
such as ALS, rely on past user experiences to generate accurate recommendations, we accounted
for scenarios where users have no historical data. In such cases, instead of failing to provide
recommendations, we redirect the user to a page where the top 5 generic asins are displayed.

By offering these generic predictions, users can still benefit from some initial recommendations
based on popular or frequently chosen products. This approach aims to enhance the user
experience by providing valuable suggestions even in the absence of personalized historical data.
As users engage more with the platform and generate new interactions, the collaborative filtering
model can progressively incorporate their preferences to offer more accurate and tailored
recommendations over time

## Live Streaming
We successfully integrated Apache Kafka into my web-based application, which generated live
user data.
### Consumer Setup for Real-time Streaming:
My code shows the configuration and setup of a Kafka Consumer. By specifying the bootstrap
servers, group ID, and offset reset, the Consumer is prepared to receive messages from the
specified topic ('test'). The Consumer subscribes to the topic and continuously polls for new
messages using the poll function. If a message is received without errors, it is processed and
yielded as a real-time event in the event_stream function.
### Event Stream Function and Streaming Route:
The event_stream function is defined within the '/stream' route of the Flask web application. This
function utilizes an infinite loop to continuously receive messages from the Kafka Consumer.
Once a message is received, it is processed and transformed into a streamable format. The
processed message, representing a real-time event, is then yielded to the web client as part of a
Server-Sent Events (SSE) response. This enables the web application to stream recommendations
based on user preferences in real time.
### Kafka Producer Setup and Sending Recommendations:
The second code snippet focuses on the Kafka Producer setup and message delivery. The
Producer is initialized with the bootstrap servers configuration. Within the '/result' route, which
handles user interactions and recommendation generation, the top 5 ASINs (Amazon Standard
Identification Numbers) are obtained. These ASINs are transformed into a list format suitable for
JSON serialization. The Producer then sends the top 5 ASINs to the Kafka topic ('test') using the
produce function. Upon successful delivery, the delivery_report function is called to handle the
delivery status of the message.

## License
This project is licensed under the [MIT License](https://opensource.org/license/MIT)- see the LICENSE file for details.

