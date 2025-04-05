# Real Estate Scraper – Almaty, Kazakhstan

![Project Illustration](./readme_bg.jpg)

## Overview

This project is a high-performance and yet simple web scraper developed to extract real estate listings from [krisha.kz](https://krisha.kz) – the most popular real estate platform in Kazakhstan. It supports scraping for multiple deal types and property types (rent/sale for apartments and houses), and it stores results in partitioned monthly folders in the efficient Parquet format.

The scraper is modular, validates incoming data structures, and is optimized for concurrent processing using Python’s multiprocessing capabilities.

---

## Features

- ✅ **Multi-city, Multi-property-type Support**
- 🚀 **Concurrent URL Processing** via `multiprocessing.Pool`
- 🧠 **Data Validation** with Pydantic models
- 🌍 **District Detection** using GeoJSON and Shapely
- 📂 **Monthly Output Partitioning**
- 🗂 **Chunked Storage in Parquet Format**
- 🧹 **Auto-cleaning Old Downloads**

---

## Technologies Used

- Python 3.10+
- Pandas
- Requests
- Shapely
- Pydantic
- Multiprocessing
- GeoJSON

---

## Author
Vitaliy Ponomaryov  
📍 Almaty, Kazakhstan

---

## License
This project is licensed for personal and portfolio demonstration purposes.
