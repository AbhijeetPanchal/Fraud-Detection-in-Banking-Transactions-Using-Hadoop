🚨 Real-Time Fraud Detection in Banking Systems — End-to-End Big Data Pipeline
This project presents a real-time, containerized fraud detection system tailored for financial transactions. Built using a modern big data architecture, it emulates ATM transaction streams, processes them using Apache Spark's machine learning capabilities, and delivers insights through interactive dashboards in Power BI. The entire solution is orchestrated via Docker Compose for seamless deployment and scalability.

📌 Highlights
Continuous data ingestion through Apache Kafka from a custom-built ATM simulator.

Real-time data streaming and fraud prediction using Apache Spark Streaming integrated with Spark MLlib.

Tiered data lake design using HDFS to manage raw (bronze) and refined (silver) datasets.

Automated transfer of curated data into HBase using Apache NiFi workflows.

Real-time fraud analytics visualized using Power BI dashboards.

Fully containerized ecosystem using Docker and Docker Compose for easy setup and management.

🧩 System Architecture
Below is the architecture diagram illustrating how various components interact:

<center> <img src="/Architecture/Picture2.png" alt="System Architecture" width="990px" height="450px"> </center>
⚙️ Technologies Used
Component Category	Tools & Frameworks
🔌 Data Ingestion	Apache Kafka, Zookeeper, Python-based ATM Transaction Generator
🔄 Stream Processing	Apache Spark Streaming, Spark MLlib, HDFS
🧠 ETL & Feature Engineering	PySpark, Spark SQL
🛠️ Orchestration	Apache NiFi (ListHDFS → FetchHDFS → ConvertRecord → PutHBaseJSON)
💾 Storage	HDFS (for tiered data), HBase (structured storage)
📊 Visualization	Power BI via JDBC/ODBC
📦 Infrastructure	Docker, Docker Compose, and internal Docker networking

🛰️ Kafka Messaging Setup
Kafka Broker: Single-node setup

Zookeeper: Single-node setup

Topics used:

raw_transactions → holds unprocessed streaming data

cleaned_transactions → holds fraud-labeled transactions

The ATM simulator, developed in Python, sends synthetic transactions directly to the raw_transactions topic, mimicking real-world banking activity.

🧠 Spark MLlib Workflow
Data is ingested in real time from Kafka and initially stored in HDFS (Bronze Layer)

Spark performs cleansing, transformation, and feature extraction to create Silver Layer data

A machine learning pipeline using Spark MLlib (e.g., Logistic Regression, Random Forest) predicts fraudulent behavior

The final output — flagged transactions — are published back into Kafka under cleaned_transactions topic

System flow diagrams:

<center> <img src="/spark/spark01.png" alt="Spark Flow 1" width="990px" height="450px"> </center> <center> <img src="/spark/spark02.png" alt="Spark Flow 2" width="990px" height="450px"> </center> <center> <img src="/spark/spark03.png" alt="Spark Flow 3" width="990px" height="450px"> </center>
🔁 Apache NiFi Integration
Apache NiFi automates the data flow from HDFS Silver Layer into HBase. Key configuration:

Process chain: ListHDFS → FetchHDFS → ConvertRecord → PutHBaseJSON

HBase Table: fraud_transactions

Row Key: transaction_id

NiFi Flow visual examples:

<center> <img src="/nifi/nifi-01.png" alt="NiFi Flow 1" width="990px" height="450px"> </center> <center> <img src="/nifi/nifi-02.png" alt="NiFi Flow 2" width="990px" height="450px"> </center>
📈 Interactive Dashboard via Power BI
Power BI connects directly to HBase or Spark SQL through JDBC/ODBC connectors and presents:

Real-time fraud detection summaries

Most affected customer accounts and regions

Historical anomaly trends

📘 Cover Page:

<center> <img src="/powerbi/Cover.png" alt="Dashboard Cover" width="990px" height="450px"> </center>
📊 Live Dashboard:

<center> <img src="/powerbi/Dashboard.png" alt="Dashboard Snapshot" width="990px" height="450px"> </center>
