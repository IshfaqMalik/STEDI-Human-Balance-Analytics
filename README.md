# STEDI Data Lakehouse Solution

## Overview
The STEDI team has developed a Step Trainer device that helps users practice balance exercises. This device collects data via integrated sensors and interacts with a mobile app that uses the phone's accelerometer. The data is used to train machine learning algorithms to accurately detect steps in real-time, ensuring user privacy by only including data from consenting customers.

This project demonstrates how to create a scalable data lakehouse solution using AWS Glue, S3, Python, and Spark. It curates and processes data from landing to trusted to curated zones, enabling efficient querying and machine learning model training.

---

## Project Datasets
The following datasets are processed and stored:
1. **Customer Records**: Information collected from the STEDI website and fulfillment system.
2. **Step Trainer Records**: Sensor data capturing object motion distance.
3. **Accelerometer Records**: Mobile app data capturing motion in X, Y, and Z directions.

---

## Implementation

### 1. **Landing Zone**
Raw data for customers, accelerometers, and step trainers is stored in the landing zone on AWS S3. Glue tables are created using AWS Glue Data Catalog, enabling querying with AWS Athena.

- **Customer Landing Table**: Contains raw customer data.
- **Accelerometer Landing Table**: Contains raw accelerometer data.
- **Step Trainer Landing Table**: Contains raw step trainer data.

---

### 2. **Trusted Zone**
The trusted zone sanitizes and transforms raw data to ensure it includes only relevant and consented information.

#### Glue Job Scripts:
- **`customer_landing_to_trusted.py`**:  
  Transfers customer data from the landing zone to the trusted zone, retaining only customers who consented to share their data.

- **`accelerometer_landing_to_trusted_zone.py`**:  
  Transfers accelerometer data to the trusted zone, filtering for readings linked to customers who consented to data sharing.

- **`trainer_landing_to_trusted.py`**:  
  Processes step trainer data from the landing zone and transfers it to the trusted zone. Filters records based on customer consent and matching accelerometer data.

The `customer_trusted` table was queried in Athena to verify that it contains only records of consenting customers.

---

### 3. **Curated Zone**
In the curated zone, datasets are further refined for specific analyses or machine learning tasks.

#### Glue Job Scripts:
- **`customer_trusted_to_curated.py`**:  
  Transfers customer data from the trusted zone to the curated zone, filtering for customers with accelerometer data.

- **`trainer_trusted_to_curated.py`**:  
  Creates an aggregated dataset combining step trainer and accelerometer data for the same timestamp, including only records from consenting customers.

---

## Row Count Validation

| **Zone**      | **Customer** | **Accelerometer** | **Step Trainer** | **Machine Learning** |
|---------------|--------------|-------------------|-------------------|-----------------------|
| **Landing**   | 956          | 81273             | 28680            | N/A                  |
| **Trusted**   | 482          | 40981             | 14460            | N/A                  |
| **Curated**   | 482          | N/A               | N/A              | 43681                |

---

## Folder Structure
```plaintext
├── README.md                  # Project documentation
├── scripts/                   # Glue job scripts
│   ├── customer_landing_to_trusted.py
│   ├── accelerometer_landing_to_trusted_zone.py
│   ├── trainer_landing_to_trusted.py
│   ├── customer_trusted_to_curated.py
│   ├── trainer_trusted_to_curated.py
├── sql/                       # SQL scripts for Glue tables
│   ├── customer_landing.sql
│   ├── accelerometer_landing.sql
│   ├── step_trainer_landing.sql
├── data/                      # Sample data files
│   ├── customer/
│   ├── accelerometer/
│   ├── step_trainer/



