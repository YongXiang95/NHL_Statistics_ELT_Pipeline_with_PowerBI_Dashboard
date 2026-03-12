# 🧹 Dirty Data Cleaners – NHL Data Engineering Pipeline

A modern **end-to-end data engineering pipeline** built on **Microsoft Fabric** to ingest, clean, validate, and transform National Hockey League (NHL) datasets into analytics-ready tables for business intelligence and insights.

This project demonstrates **production-style data engineering practices**, including Medallion architecture, data quality checks, pipeline orchestration, and BI delivery.

---

# 📊 Project Overview

The goal of this project was to design and implement a **scalable data pipeline** that transforms raw NHL datasets into curated datasets ready for analytics.

### Objectives

- Ingest raw NHL datasets from CSV sources
- Implement **Medallion Architecture (Bronze → Silver → Gold)**
- Perform **data cleaning and validation**
- Apply **data modeling for analytics**
- Deliver **analytics-ready datasets** for **Power BI dashboards**

### Example Analyses Enabled

- NHL **player performance analysis**
- **Game outcomes and seasonal trends**
- **Player efficiency metrics** (e.g., goals per 60 minutes)

**Focus of Project**

| Area | Focus |
|-----|-----|
| Data Engineering | 90% |
| Data Analysis & Visualization | 10% |

---

# 📌 Project Highlights

✔ End-to-end data engineering pipeline  
✔ Medallion architecture implementation  
✔ Data quality validation framework  
✔ Fabric pipeline orchestration  
✔ Analytics dashboards in Power BI  

---

# 🏗 Architecture

The pipeline follows the **Medallion Data Architecture** pattern.

```
Raw CSV Data
      │
      ▼
Bronze Layer
(Raw ingestion)
      │
      ▼
Silver Layer
(Data cleaning + validation)
      │
      ▼
Gold Layer
(Analytics-ready tables)
      │
      ▼
Power BI Dashboards
```

### Data Model Strategy

| Layer | Model |
|------|------|
| Silver | **Galaxy Schema** (optimized for ingestion and transformation) |
| Gold | **Wide tables** (optimized for analytics queries) |

---

# ⚙️ Tech Stack

| Category | Tools |
|---------|------|
| Data Platform | **Microsoft Fabric** |
| Data Processing | **PySpark** |
| Storage | **Fabric Lakehouse (Delta Tables)** |
| Warehouse | **Fabric Data Warehouse** |
| Data Quality | **Great Expectations**, custom PySpark checks |
| Orchestration | **Fabric Pipelines** |
| Visualization | **Power BI** |
| Version Control | **GitHub** |

---

# 🔄 Pipeline Orchestration

The project implements a **Fabric pipeline** responsible for executing the full transformation workflow.

### Current Capabilities

- End-to-end pipeline:  
  **Bronze → Silver → Gold**
- Historical data loads
- Data validation checks
- Table validation before downstream consumption

### Future Improvements

- Incremental loads
- Idempotent pipeline execution
- Automated deployment pipelines

---

# 🧪 Data Quality Framework

Data quality checks were implemented to ensure **reliable and trustworthy data** before it reaches the analytics layer.

### Why Data Quality Checks?

- Detect data issues before stakeholders
- Prevent corrupted data from propagating downstream
- Reduce compute cost by stopping faulty pipelines early
- Improve debugging and observability

---

## Silver Layer Checks

Examples of validations implemented:

- Primary key uniqueness checks
- Referential integrity checks (Fact ↔ Dimension)
- Positive numeric value validation
- Aggregate validation checks
- Table completeness checks

---

## Gold Layer Checks

Implemented using **Great Expectations**.

Checks include:

- Positive numeric values
- Expected column counts
- Valid datetime ranges
- Category value validation
- Data within reasonable ranges

---

# 🔍 Exploratory Data Analysis

Before building transformations, extensive **EDA** was performed to understand the structure and issues in the NHL datasets.

### Examples of Issues Found

#### Missing Data

Example: `game_skater_stats`

- >40% null values for:
  - hits
  - takeaways
  - giveaways
  - blocked shots

**Decision:**

- Avoid imputing unreliable estimates
- Use **non-null values for downstream aggregations**

---

#### Dataset Issues Identified

| Dataset | Key Findings |
|-------|-------------|
| `player_info` | Missing nationality, birth location, height/weight |
| `game_shifts` | Missing shift_end timestamps |
| `game_teams_stats` | Numeric values stored as strings |
| `game` | Duplicate rows and missing rink side values |

**Example decisions:**

- Remove duplicate rows
- Keep missing categorical values where imputation is unreliable
- Correct incorrect data types

---

# 📊 Data Visualizations

Curated Gold datasets were connected to **Power BI dashboards**.

### Dashboards Built

#### Goalie Performance Dashboard

Features:

- Normalized statistics per **60 minutes**
- Filterable player metrics
- Comparative analysis

---

#### Skater Performance Dashboard

Metrics include:

- Goals per 60
- Shots per 60
- Conversion efficiency

---

### Player Performance Insights

Example insight discovered:

**Most improved player (2015–2020)**

🏒 **David Pastrnak**

Measured using:

- Percentage change in **goals per 60**
- Year-on-year improvement analysis

---

# ⚠️ Key Engineering Challenges

## 1️⃣ Pipeline Parameters

**Challenge:**  
Passing parameters between pipeline activities.

Example use case:

Passing the **list of staging tables created** in one activity to the next pipeline step responsible for publishing data.

This enabled **metadata-driven pipeline behavior**.

---

## 2️⃣ Compute Optimization

Each Spark action has compute cost:

- Reading CSV files
- Reading lakehouse tables
- Complex transformations
- Cluster startup time

**Solutions included:**

- Reducing unnecessary reads
- Optimizing notebook steps
- Consolidating transformations

---

## 3️⃣ Lakehouse vs Warehouse in Fabric

Key differences discovered:

| Feature | Lakehouse | Warehouse |
|------|------|------|
| Engine | Spark | SQL |
| Table Type | Delta Tables | SQL Tables |
| Update/Delete via PySpark | Supported | Limited |

**Conclusion**

Use **T-SQL** for Warehouse operations like **UPDATE and DELETE**.

---

# 🧠 Key Learnings

This project reinforced several real-world data engineering principles.

### Data Engineering

- Medallion architecture design
- Data pipeline orchestration
- Data quality frameworks
- Spark performance optimization
- Metadata-driven pipelines

---

### Data Platform Concepts

- Differences between **Lakehouse vs Data Warehouse**
- Managing Spark compute costs
- Designing analytics-ready schemas

---

### Engineering Practices

- Agile Scrum workflow
- Sprint planning and task tracking
- Iterative pipeline development

---

# 🚀 Future Enhancements

Planned improvements:

- Implement **incremental loading**
- Ensure **idempotent pipeline design**
- Add **CI/CD deployment pipelines**
- Improve **Power BI performance**
- Introduce **advanced NHL analytics models**

Examples:

- Player fatigue modeling
- Performance prediction
- Advanced statistical metrics

---

# 👥 Team

**Dirty Data Cleaners**

- Yong Xiang Oh  
- Justin Pek  
- Syafiq Diah  

