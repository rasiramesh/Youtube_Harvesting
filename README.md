## YouTube Data Harvesting and Warehousing Project

## Project Overview

This project involves creating a Streamlit application for harvesting and warehousing data from multiple YouTube channels. The application offers a user-friendly interface for users to access, analyze, and store data from these channels. The data is stored in a MySQL database for structured data and a MongoDB database for unstructured data. This README will guide you through the project's workflow and how to run it.

## Technologies Used

Python
MySQL
MongoDB
Streamlit
YouTube Data API

## Project Structure

- Homepage.py: This is the main Streamlit application that serves as the user interface. Users can interact with the application here.

- Data Harvesting.py: This script is responsible for fetching data from the YouTube API. It manages the data extraction process and store in MongoDB databases.

- Data Migration.py: This script deals with migrating the data into the MySQL from MongoDB databases.

- requirements.txt: This file lists all the Python dependencies required for the project. You can install these using `pip`.

- config.py: Here, you'll set up API keys, database credentials, and other configuration options. Make sure to fill this file out before running the project.

## Workflow

Data Harvesting:
The application uses the YouTube Data API to fetch data from selected channels.
Collected data includes video information, view counts, likes, and comments.

Data Storage:
Data is stored in two databases:
MySQL: Structured data such as video details.
MongoDB: Unstructured data like video comments.

Streamlit Dashboard:
Users access the data and analytics through a Streamlit web application.

## Application Usage

- Start the Streamlit application to access, analyze, and visualize the data. Use the following command:
   streamlit run Homepage.py
- Access the Streamlit application at the provided URL (usually `http://localhost:8501`).
- Enter the YouTube channels you want to explore Data Harvesting page.
- Select the YouTube channels you want to Migrate Data Migration page.
- The Dashboard page helps to analyze, and visualize the data .

## Conclusion

This project allows you to efficiently harvest and warehouse data from YouTube channels and provides a Streamlit interface for easy data exploration. You can easily customize and expand the application to meet your specific requirements.

