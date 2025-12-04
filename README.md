Real-World Banking ETL

This project is an etl assignment, where the goal is to build a SQL-based ETL pipeline that automates how a bank processes monthly vehicle data and generates profit estimates for auto loans.

📌 Background

Each month, a third-party agency provides an updated file containing vehicle details such as model names, energy classes (electric, gasoline, diesel), and technical specifications.
Historically, a business analyst manually executed SQL queries to calculate profit estimates based on the car’s energy class, the floating monthly interest rate, and the bank’s margin.

🎯 Purpose

The goal is to fully automate this workflow using SQL Server and SQL Server Agent, removing the need for any manual queries.

✅ What the ETL Does

The SQL-based ETL pipeline automatically:

Ingests the monthly vehicle data into a staging table.

Validates and transforms the data (correct types, clean formats, consistent values).

Applies business logic using updated interest rates and margin tables.

Loads the final enriched dataset into a monthly table

A SQL Server Agent Job runs the entire ETL pipeline on a schedule, ensuring each month’s file is processed without manual intervention.

The business analyst can then simply run:

SELECT * 
FROM LoanProfitEstimates_yyyymm;
