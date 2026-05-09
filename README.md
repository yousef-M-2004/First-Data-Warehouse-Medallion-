# First-Data-Warehouse-Medallion-

🏗️ Modern SQL Data Warehouse Project
Welcome to my end-to-end Data Engineering project! This repository showcases the complete lifecycle of building a modern SQL Data Warehouse from scratch using SQL Server.

🚀 Project Overview
This project follows industry best practices to transform raw, disconnected source data into a business-ready dimensional model. It demonstrates proficiency in data architecture, ETL processes, quality assurance, and technical documentation.

🏗️ Architecture Design
The warehouse is built upon a modular three-layer architecture:

🟤 Bronze Layer (Raw): Direct ingestion of source data from CRM and ERP systems (CSV formats).
⚪ Silver Layer (Cleaned): Data transformation, standardization, and rigorous quality validation.
🟡 Gold Layer (Analytics): Business-focused Star Schema modeling (Facts & Dimensions) optimized for BI tools.
🛠️ Technical Stack
Database: SQL Server / SSMS
Data Modeling: Draw.io (Architecture & Data Lineage)
Project Management: Notion
Version Control: Git / GitHub
📂 Repository Structure
text
1)scripts      # DDLs for all layers and ETL stored procedures
2)tests        # Data quality and integrity validation scripts
3)docs         # Architecture diagrams, data catalog, and models

🔑 Key Deliverables
Automated ETL: Robust stored procedures for scalable data ingestion.
Data Quality Gates: Automated checks for nulls, duplicates, and invalid formats.
Comprehensive Documentation: Includes detailed data flow diagrams and a functional data catalog.
Check out the project roadmap for more details.
