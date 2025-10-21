# NYC Taxi Data Pipeline

## 🌍 Overview

This repository provides a data pipeline for processing **NYC Taxi** trip data. The pipeline uses **Docker**, **PostgreSQL**, **MongoDB**, and **FastAPI** to:

1. Load taxi data from **PostgreSQL**.
2. Clean the data (remove outliers, handle missing values, etc.).
3. Store the cleaned data in **MongoDB**.
4. Expose the cleaned data via a **FastAPI** service.

---

## ⚙️ Technologies Used

- **Docker**: Containerization for services.
- **PostgreSQL**: Relational database for storing raw NYC taxi data.
- **MongoDB**: NoSQL database for storing cleaned taxi trip data.
- **FastAPI**: Python framework for building APIs.
- **pandas**: Data manipulation and cleaning library in Python.
- **SQLAlchemy**: SQL toolkit for Python to interact with PostgreSQL.

---

## 📦 Setup & Installation

### 1. Clone the repository

```bash
git clone https://github.com/yourusername/nyc-taxi-data-pipeline.git
cd nyc-taxi-data-pipeline
