# Cleaning-an-e-commerce-dataset-with-PySpark

# **Cleaning an Orders Dataset with PySpark**

## **Overview**
This project demonstrates the use of **PySpark**, a distributed big data processing framework, to clean and preprocess a dataset containing order information for an e-commerce company. The goal is to prepare the data for downstream applications, such as demand forecasting models and business analytics.

This project highlights:
- **Big Data Processing:** Handling and transforming large datasets using PySpark's distributed capabilities.
- **Data Cleaning:** Removing, formatting, and restructuring raw data into a usable format.
- **Scalability:** Leveraging PySpark's performance optimizations for processing large-scale datasets.

---

## **Project Context**
You are a Data Engineer at Voltmart, an electronics e-commerce company. The Machine Learning team has requested cleaned data for building a **demand forecasting model**. You are tasked with:
- Cleaning and preprocessing the raw data stored in **Parquet format**.
- Delivering the cleaned dataset in the desired format, ready for machine learning pipelines.

---

## **Objectives**
The project involves:
1. **Data Loading**:
   - Reading a raw dataset stored in Parquet format.
2. **Data Cleaning and Preprocessing**:
   - Adding a new column categorizing the time of day (morning, afternoon, evening).
   - Filtering out orders placed between midnight and early morning.
   - Standardizing `product` and `category` column values by converting them to lowercase.
   - Excluding discontinued products (e.g., "TVs").
   - Extracting the **state** from the `purchase_address` column for location-based analysis.
3. **Data Export**:
   - Saving the cleaned dataset in Parquet format for optimized querying and storage.

---

## **Technology Stack**
- **PySpark**: For scalable, distributed data processing.
- **Parquet**: For efficient, compressed data storage.
- **Python**: For coding and workflow automation.

---

## **Features**
### **1. Time-of-Day Categorization**
Added a `time_of_day` column to classify orders into:
- `Morning`: 6 AM to 11 AM
- `Afternoon`: 12 PM to 5 PM
- `Evening`: 6 PM to 11 PM

### **2. Filtering and Formatting**
- Removed orders placed at `night` (12 AM to 5 AM) as they are less relevant for forecasting.
- Standardized text fields (`product`, `category`) to lowercase for uniformity.

### **3. Discontinued Products**
- Filtered out orders for discontinued products like "TVs."

### **4. Location-Based Insights**
- Extracted `state` abbreviations from the `purchase_address` column to enable geographic analysis.

---

## **Data Workflow**
1. **Input**: 
   - Raw dataset in Parquet format.
2. **Processing**:
   - Applied a series of PySpark transformations to clean and preprocess the data.
3. **Output**:
   - Cleaned dataset saved as a Parquet file, partitioned for efficient querying and storage.

---

## **Dataset**
- **Input File**: `orders_data.parquet`
- **Output File**: `orders_data_clean.parquet`

### **Sample Schema**
| Column               | Description                                                  |
|-----------------------|--------------------------------------------------------------|
| `order_date`          | Date and time of the order                                   |
| `order_id`            | Unique identifier for each order                            |
| `product`             | Name of the product                                         |
| `category`            | Product category                                            |
| `purchase_address`    | Full address of the customer                                |
| `quantity_ordered`    | Number of units ordered                                     |
| `price_each`          | Price per unit                                              |
| `purchase_state`      | State extracted from the `purchase_address`                 |
| `time_of_day`         | Categorized time of day (morning, afternoon, evening)       |

---

## **Key Metrics**
- **Number of Unique States**: Extracted `n_states` = `8` states where orders were placed.
- **Removed Orders**: Filtered out orders placed at night and for discontinued products.

---

## **Results**
The cleaned dataset:

Contains only relevant data (filtered for discontinued products and specific time ranges).
Is optimized for storage and analysis through Parquet format.
Enables downstream tasks like machine learning and business analytics.
