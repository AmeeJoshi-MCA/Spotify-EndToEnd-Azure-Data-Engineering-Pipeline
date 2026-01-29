# Spotify Real-Time Data Engineering Pipeline (Azure & Databricks)

## 📌 Project Overview
This repository contains a production-grade Data Engineering pipeline built on the **Microsoft Azure** ecosystem. The project transforms raw Spotify clickstream data into analytics-ready datasets using a **Medallion Architecture** (Bronze, Silver, Gold).

The technical core of this project is the implementation of **Delta Live Tables (DLT)** and **Change Data Capture (CDC)** to maintain a high-performance, incremental Gold layer, all managed via **Databricks Asset Bundles (DABs)** for a professional CI/CD workflow.

## Architecture

![Recording 2026-01-29 112918](https://github.com/user-attachments/assets/539b8e2a-be40-4de6-8415-21528145a712)


## 🛠️ Tech Stack & Key Keywords
* **Cloud Platform:** Microsoft Azure (ADLS Gen2, Azure SQL, ADF)
* **Data Processing:** Azure Databricks (Spark, Delta Live Tables)
* **Compute:** Azure Databricks (Spark & Delta Live Tables)
* **Orchestration:** Azure Data Factory with Watermark-based Incremental Loading
* **Deployment:** Databricks Asset Bundles (DABs) for production-ready Infrastructure as Code (IaC).
* **Security:** Managed Identity-based access to eliminate the need for sensitive credentials.

## 🏗️ Implementation Details

### Phase 1: Ingestion & Bronze Layer
* **Incremental ELT:** Configured ADF to move data from Azure SQL Database to ADLS Gen2.
* **Watermarking Logic:** Implemented a Lookup-based watermark pattern to process only new records based on the last processed timestamp.

 <img width="2081" height="1084" alt="image" src="https://github.com/user-attachments/assets/81d15ef8-57c0-480f-9213-e2e74b5a79c7" />


### Phase 2: Transformation & Silver Layer

The Silver layer is implemented using Azure Databricks + Spark Structured Streaming and follows a file-based Delta Lake approach.

Instead of directly creating managed catalog tables first, this project writes and maintains Delta files at explicit storage paths (URIs) in ADLS Gen2. These paths act as the system of record, and tables are registered in Unity Catalog only after the data is stabilized.

**Key Silver Layer Design Decisions**

   * Delta files are read and written using explicit storage paths (URIs)  
   * Upsert (MERGE) logic is applied at the file level, not catalog-first
   * Streaming micro-batches are handled using foreachBatch
   * First load creates Delta files; subsequent loads perform MERGE-based upserts  
   * Unity Catalog tables are created on top of existing Delta files (external tables)

**Why This Approach?**
  
  * Decouples physical storage from logical table definitions
  * Safer and more flexible for streaming + incremental pipelines
  * Aligns with real-world lakehouse patterns used in production
  * Avoids early dependency on catalog availability
  * Reference Implementation: A dedicated Silver writer class handles path-based Delta upserts, streaming checkpoints, and later registration into Unity Catalog. (See Writer and Reader implementation in the codebase for details.)

  
### Phase 3: Gold Layer & CDC (The "DABs" Advantage)

The Gold layer is built using **Databricks Delta Live Tables (DLT)** pipelines and represents the **final, analytics-ready datasets**.

Unlike ad-hoc notebook transformations, this project uses **declarative DLT pipelines** to apply **CDC-based transformations** consistently across all fact and dimension tables.

### Gold Layer Design Pattern

For each Gold table, the following standardized pattern is implemented:

1. **Streaming Staging Table**

   * Reads from the Silver layer using `readStream`
   * Configured with `ignoreChanges = true` to safely consume MERGE-based Delta updates
   * Acts as a controlled staging layer for CDC processing

2. **Explicit Target Table Declaration**

   * Target Gold tables are explicitly declared as streaming tables
   * Prevents runtime dependency and ordering issues during pipeline execution

<img width="2978" height="838" alt="image" src="https://github.com/user-attachments/assets/19e022d1-7dcb-4c65-8dbd-c6028882fb2d" />

3. **CDC Application Using `apply_changes`**

   * Uses primary keys and sequence columns
   * Implements **SCD Type 1** logic
   * Automatically handles inserts and updates

<img width="1905" height="1163" alt="image" src="https://github.com/user-attachments/assets/0e879b2f-0b6d-4b96-a86b-e0ee7f5a136c" />



### Why Delta Live Tables?

* Declarative, production-grade pipeline management
* Built-in data quality, lineage, and dependency handling
* Simplified CDC implementation at scale
* Clear separation between staging and curated layers

> **Reference Implementation:**
> All Gold tables follow a consistent DLT pattern using staging tables and `apply_changes` logic. See the Gold layer DLT pipeline definitions for table-specific configurations.

---

## 🔐 Security & IAM (Concise)

* Azure Managed Identities used for ADF and Databricks (no secrets)
* RBAC enforced on ADLS Gen2 using least-privilege access
* Unity Catalog controls access between Silver (read) and Gold (write) layers
* External locations secured via managed identity
* Optional integration with Azure Key Vault for API secrets

## ⚙️ Performance & Reliability (Concise)

* Incremental ingestion using CDC / watermarks
* Delta Lake ensures ACID transactions and schema enforcement
* DLT pipelines provide fault-tolerant, declarative Gold layer processing
* Streaming handled with checkpoints and trigger(once=True)
* Optimized reads using Delta file compaction & Z-Ordering
* Pipelines are restart-safe and idempotent

---

## 📊 Use Cases

* Incremental processing of Spotify-style streaming and user activity data
* Near–real-time fact table creation using CDC and Delta Live Tables
* User engagement and listening behavior analysis
* Track and artist popularity analytics
* Analytics-ready datasets for BI and reporting tools
* Reference architecture for cloud-scale Azure data platforms

---

## ✅ Conclusion

This project demonstrates a **production-grade Azure data engineering solution** built using modern Lakehouse principles. It showcases **incremental CDC ingestion**, **secure cloud-native orchestration**, and **scalable transformations** using **Azure Data Factory, Databricks, Delta Lake, and Delta Live Tables**.

The design emphasizes **reliability, performance, and maintainability**, following **industry-standard Bronze–Silver–Gold architecture** and best practices used in real enterprise data platforms.

Overall, this project reflects **hands-on experience with end-to-end data engineering workflows** and the ability to design **robust, analytics-ready data systems on Azure**.

---

