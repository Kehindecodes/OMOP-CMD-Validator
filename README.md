OMOP CDM Data Validator & Dashboard
This project is a Python-based application designed to validate and visualize the data quality of a dataset formatted to the OMOP Common Data Model (CDM) v5.4. It is built to help researchers and data engineers ensure their data is clean and compliant before it is used for analysis.
Key Features & Functionalities
 * Schema-Driven Validation: The core of the application is a custom validation script that uses a YAML-based schema to define and enforce rules. This schema is programmatically derived from the official OMOP CDM v5.4 DDL, ensuring accuracy and comprehensive coverage of all tables and columns.
 * Data Integrity Checks: The tool performs a wide range of checks to identify common data quality issues, including:
   * Missing Columns
   * Data Type Mismatches
   * Required Field Validation
   * Primary Key Uniqueness
   * Foreign Key Constraints
 * Interactive Web Dashboard: The application provides a simple and intuitive web interface for a smooth user experience.
   * File Upload: Users can easily upload their OMOP CDM CSV files.
   * Real-time Reporting: After validation, a live report is generated on the dashboard, providing a clear summary of all errors found, categorized by table and type.
 * Scalable Design: By separating the validation logic from the schema configuration, the application can be easily updated or extended to support new versions of the OMOP CDM or additional data quality rules.
Technologies Used
 * Python
 * FastAPI
 * Pandas
 * YAML
 * HTMX
Getting Started
To get started, follow these steps:
 * Clone the repository: git clone https://github.com/Kehindecodes/OMOP-CDM-Validator
 * Install dependencies: pip install -r requirements.txt
 * Run the application: uvicorn main:app --reload
 * Access the dashboard: Open your web browser and navigate to http://127.0.0.1:8000.
Project Structure
.
├── app/
│   ├── main.py          # FastAPI application entry point
│   ├── validator.py     # Contains the core Validator class
│   └── visualizer.html  # HTML template for the HTMX dashboard
├── data/
│   └── ...              # Placeholder for OMOP CSV files
├── config/
│   └── omop_schema.yaml   # The YAML schema file
├── requirements.txt
└── README.md

References
 * OMOP Common Data Model
 * OMOP CDM v5.4 DDL (PostgreSQL)
