#  News Data Aggregation Pipeline

## Overview
This project focuses on building a pipeline that aggregates and processes real-time news trends. The system is built using a multithreaded, distributed architecture to efficiently scrape news data, preprocessing it, and pipelining it into data stores. The project demonstrates expertise in web scraping, distributed systems, data transformation, and multithread management, all while leveraging technologies such as Kafka, PostgreSQL, and Java.

## Key Features

### 1. Multithreaded Web Scraper
- **Architecture**: Designed using a manager-worker event-driven architecture for scalability.
- **Functionality**: The web scraper extracts real-time news data from multiple sources. The manager distributes tasks to worker threads to ensure efficient data collection across the web.

### 2. Data Transformation and Preprocessing
- **Data Transfer**: Scraped data is batched and sent to a PostgreSQL database.
- **Multithreaded Java Application**: A custom-built Java application is triggered to preprocess the news data in parallel, ensuring fast and efficient handling of large datasets.
- **Kafka Integration**: Preprocessed data is ingested into a Kafka pipeline, enabling real-time streaming of the news data for further analysis.


## Technologies Used
- **Java (Multithreading)**: Built a robust backend system to handle concurrent processing and task distribution.
- **PostgreSQL**: Managed data persistence, ensuring consistent and accurate storage of batched news data.
- **Kafka**: Implemented a scalable pipeline for streaming data, enabling real-time trend analysis.

## Concepts and Skills Learned
- **Distributed Systems & Multithreading**: Implemented manager-worker architecture to efficiently scrape and process real-time data.
- **Event-Driven Architecture**: Developed an event-driven system using Kafka for seamless data ingestion and pipeline management.
- **Data Pipeline Development**: Gained hands-on experience in building and managing a streaming data pipeline from web scraping to machine learning-driven trend aggregation.

## Conclusion
This project showcases advanced skills in distributed systems, multithreading, and data pipeline development. By combining real-time web scraping and data processing, it delivers a powerful solution for tracking real-time news data.
