# NYC Yellow Taxi 2025 Analysis using Rust and DataFusion

This project performs analytical processing on the NYC TLC Yellow Taxi Trip Records for the year 2025 using the Rust programming language and Apache DataFusion. The application loads monthly Parquet files, registers them as a single DataFrame, and computes required aggregations using both the DataFusion DataFrame API and SQL API.

---

## Project Objectives
- Load all 12 monthly NYC Yellow Taxi Parquet files for 2025.  
- Register the data in DataFusion as a unified logical table.  
- Perform the required aggregations using both DataFrame API and SQL.  
- Display the results in a readable terminal format.  
- Save the final output screenshot in the `screenshots/` folder.  

---

## Dataset Source
NYC TLC Trip Record Data  
https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page

Data Dictionary  
https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf

---

## How to Download the Data
1. Visit the NYC TLC Trip Record Data webpage.  
2. Under "Yellow Taxi Trip Records," download each Parquet file for January to December 2025.  
3. Place all Parquet files inside a folder named `data` located in the project directory.  
4. Ensure filenames follow the standard TLC format:  
   ```
   yellow_tripdata_2025-01.parquet
   yellow_tripdata_2025-02.parquet
   yellow_tripdata_2025-03.parquet
   ...
   yellow_tripdata_2025-12.parquet
   ```

---

## How to Run the Project

Use the following command in the terminal:

```
cargo run
```

The application automatically loads all Parquet files present in the `data/` folder.

---

## Aggregations Performed

### 1. Trips and Revenue by Month  
Grouped by pickup month (derived from `tpep_pickup_datetime`).

Computed:
- Trip count  
- Total revenue (sum of `total_amount`)  
- Average fare (avg of `fare_amount`)  
- Sorted by month in ascending order  

Implemented using:
- DataFrame API  
- SQL API  

---

### 2. Tip Behavior by Payment Type  
Grouped by `payment_type`.

Computed:
- Trip count  
- Average tip amount  
- Tip rate calculated as sum(tip_amount) / sum(total_amount)  
- Sorted by trip count in descending order  

Implemented using:
- DataFrame API  
- SQL API  

---

## Output Screenshot

The required screenshot includes:
- Results from both DataFrame API and SQL  
- Program execution confirmation:  
  **All aggregations completed successfully**

Location in the repository:
```
screenshots/output.png
```

---

## Repository Structure

```
nyc_yellow_taxi_2025/
│
├── data/                     (ignored using .gitignore; not uploaded)
├── screenshots/
│   └── output.png
├── src/
│   └── main.rs
├── .gitignore
├── Cargo.toml
└── README.md
```

---

## Notes
- The `data/` folder is excluded using `.gitignore` so that Parquet files are not uploaded to GitHub.  
- The project uses Apache DataFusion to compute both SQL and DataFrame aggregations.  
- All assignment requirements have been implemented successfully.

