# STEDI-Human-Balance-Analytics

The STEDI team has developed an innovative Step Trainer device that helps users practice balance exercises. The device collects data via integrated sensors and interacts with a mobile app that utilizes the phone's accelerometer. This data is used to train machine learning algorithms to accurately detect steps in real-time.

STEDI has gained a significant number of early adopters who are excited about using this technology. A subset of these users has agreed to share their data for research purposes, allowing STEDI to build a robust dataset while maintaining user privacy. Only the data from these consenting customers is included in the machine learning training dataset.
#Project Description
This project involves curating and processing the data generated by the STEDI Step Trainer and its companion mobile app to create a lakehouse solution. This solution provides a centralized repository of well-structured data for data scientists to train machine learning models.

The lakehouse is built using AWS Glue, AWS S3, Python, and Spark, and is designed to handle semi-structured sensor data. The project involves setting up storage zones (landing, trusted, and curated), performing data transformations, and organizing datasets to be queried efficiently.
# Project Description
This project involves curating and processing the data generated by the STEDI Step Trainer and its companion mobile app to create a lakehouse solution. This solution provides a centralized repository of well-structured data for data scientists to train machine learning models.

The lakehouse is built using AWS Glue, AWS S3, Python, and Spark, and is designed to handle semi-structured sensor data. The project involves setting up storage zones (landing, trusted, and curated), performing data transformations, and organizing datasets to be queried efficiently.

# Project Datasets
The project utilizes three primary datasets:

Customer Records: Collected from the STEDI website and fulfillment system.
Step Trainer Records: Sensor data capturing object motion distance.
Accelerometer Records: Mobile app data capturing motion in X, Y, and Z directions.
