/*
---------------------------------------------------------
Federal Sales SQL Project
---------------------------------------------------------
Objective:
Clean, transform, and analyze federal sales data for oil, gas, and natural gas liquids
reported by the U.S. Department of the Interior. The goal is to uncover trends in 
royalty revenue, evaluate regional and land-type performance, and calculate 
efficiency metrics across calendar years 2013–2023.

Source: U.S. Department of the Interior – Office of Natural Resources Revenue (ONRR)
Dataset: Calendar Year Federal Sales of U.S. Natural Resources
URL: https://catalog.data.gov/dataset/calendar-year-federal-sales-of-u-s-natural-resources

Database: PostgreSQL
Schema: "Project"
Author: [Kevin Hernandez]
---------------------------------------------------------
*/

-- ========================================
-- Set working schema
-- ========================================
SET search_path TO "Project";

-- ========================================
-- Create a cleaned working copy of the dataset
-- ========================================
CREATE TABLE federal_sales_clean AS
SELECT *
FROM federal_sales;

-- ========================================
-- Convert key financial and rate columns to NUMERIC for precision
-- ========================================
ALTER TABLE federal_sales_clean
ALTER COLUMN "Sales Volume" TYPE NUMERIC(20,2) USING "Sales Volume"::NUMERIC,
ALTER COLUMN "Gas MMBtu Volume" TYPE NUMERIC(20,2) USING "Gas MMBtu Volume"::NUMERIC,
ALTER COLUMN "Sales Value" TYPE NUMERIC(20,2) USING "Sales Value"::NUMERIC,
ALTER COLUMN "Royalty Value Less Allowances (RVLA)" TYPE NUMERIC(20,2) USING "Royalty Value Less Allowances (RVLA)"::NUMERIC,
ALTER COLUMN "Transportation Allowances (TA)" TYPE NUMERIC(20,2) USING "Transportation Allowances (TA)"::NUMERIC,
ALTER COLUMN "Processing Allowances (PA)" TYPE NUMERIC(20,2) USING "Processing Allowances (PA)"::NUMERIC,
ALTER COLUMN "Effective Royalty Rate" TYPE NUMERIC(5,4) USING "Effective Royalty Rate"::NUMERIC;

-- ========================================
-- Replace NULLs in numeric columns with zeros to ensure completeness
-- ========================================
UPDATE federal_sales_clean
SET 
    "Sales Volume" = COALESCE("Sales Volume", 0),
    "Gas MMBtu Volume" = COALESCE("Gas MMBtu Volume", 0),
    "Sales Value" = COALESCE("Sales Value", 0),
    "Royalty Value Less Allowances (RVLA)" = COALESCE("Royalty Value Less Allowances (RVLA)", 0),
    "Transportation Allowances (TA)" = COALESCE("Transportation Allowances (TA)", 0),
    "Processing Allowances (PA)" = COALESCE("Processing Allowances (PA)", 0),
    "Effective Royalty Rate" = COALESCE("Effective Royalty Rate", 0);

-- ========================================
-- Standardize text fields: uppercase, trim whitespace
-- ========================================
UPDATE federal_sales_clean
SET 
    "Land Class" = UPPER(TRIM("Land Class")),
    "Land Category" = UPPER(TRIM("Land Category")),
    "State/Offshore Region" = UPPER(TRIM("State/Offshore Region")),
    "Revenue Type" = UPPER(TRIM("Revenue Type")),
    "Commodity" = UPPER(TRIM("Commodity"));

-- ========================================
-- Remove duplicate or excessive internal spacing in text columns
-- ========================================
UPDATE federal_sales_clean
SET 
    "Land Category" = REGEXP_REPLACE("Land Category", '\s+', ' ', 'g'),
    "Land Class" = REGEXP_REPLACE("Land Class", '\s+', ' ', 'g'),
    "State/Offshore Region" = REGEXP_REPLACE("State/Offshore Region", '\s+', ' ', 'g'),
    "Revenue Type" = REGEXP_REPLACE("Revenue Type", '\s+', ' ', 'g'),
    "Commodity" = REGEXP_REPLACE("Commodity", '\s+', ' ', 'g');

-- ========================================
-- Remove trailing punctuation from string fields for clean joins/grouping
-- ========================================
UPDATE federal_sales_clean
SET 
    "Land Category" = REGEXP_REPLACE("Land Category", '[\.,]+$', '', 'g'),
    "Land Class" = REGEXP_REPLACE("Land Class", '[\.,]+$', '', 'g'),
    "State/Offshore Region" = REGEXP_REPLACE("State/Offshore Region", '[\.,]+$', '', 'g'),
    "Revenue Type" = REGEXP_REPLACE("Revenue Type", '[\.,]+$', '', 'g'),
    "Commodity" = REGEXP_REPLACE("Commodity", '[\.,]+$', '', 'g');

-- ========================================
-- Standardize separators like " / " or " - " to consistent formats
-- ========================================
UPDATE federal_sales_clean
SET 
    "Land Category" = REPLACE(REPLACE("Land Category", ' - ', '-'), ' / ', '/'),
    "Land Class" = REPLACE(REPLACE("Land Class", ' - ', '-'), ' / ', '/'),
    "Revenue Type" = REPLACE(REPLACE("Revenue Type", ' - ', '-'), ' / ', '/'),
    "Commodity" = REPLACE(REPLACE("Commodity", ' - ', '-'), ' / ', '/');

-- ========================================
-- Correct specific known typos or inconsistent values manually
-- ========================================
UPDATE federal_sales_clean
SET "Revenue Type" = 
    CASE 
        WHEN "Revenue Type" = 'INTERGOVEN REVENUE-FEDERAL' THEN 'INTERGOVERN REVENUE-FEDERAL'
        WHEN "Revenue Type" = 'INTERGOVEN REVENUE-STATE' THEN 'INTERGOVERN REVENUE-STATE'
        ELSE "Revenue Type"
    END;

-- ========================================
-- Add a derived metric: Royalty Efficiency (Royalty / Sales * 100)
-- ========================================
ALTER TABLE federal_sales_clean ADD COLUMN royalty_efficiency NUMERIC(6,2);

UPDATE federal_sales_clean
SET royalty_efficiency = 
    CASE 
        WHEN "Sales Value" = 0 THEN NULL 
        ELSE ROUND(("Royalty Value Less Allowances (RVLA)" / "Sales Value") * 100, 2)
    END;

-- ========================================
-- Record count check: Total rows in cleaned dataset
-- ========================================
SELECT COUNT(*) AS total_records FROM federal_sales_clean;

-- ========================================
-- Detect possible duplicates based on key financial and location fields
-- ========================================
WITH dup_check AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY "Calendar Year", "State/Offshore Region", "Sales Value", "Royalty Value Less Allowances (RVLA)"
               ORDER BY "Sales Value"
           ) AS rn
    FROM federal_sales_clean
)
SELECT *
FROM dup_check
WHERE rn > 1;

-- ========================================
-- Annual royalty revenue trend
-- Purpose: Analyze how total royalty revenue has changed year over year
-- ========================================
SELECT "Calendar Year", 
       ROUND(SUM("Royalty Value Less Allowances (RVLA)")) AS total_royalties
FROM federal_sales_clean
GROUP BY "Calendar Year"
ORDER BY "Calendar Year";

-- ========================================
-- Top 10 states or offshore regions by total royalties
-- Question: Which states or offshore regions generated the most royalty revenue?
-- ========================================
SELECT "State/Offshore Region", 
       ROUND(SUM("Royalty Value Less Allowances (RVLA)")) AS total_royalties
FROM federal_sales_clean
GROUP BY "State/Offshore Region"
ORDER BY total_royalties DESC
LIMIT 10;

-- ========================================
-- Average royalty rate by land category
-- Question: How do royalty rates vary by land category?
-- ========================================
SELECT "Land Category", 
       ROUND(AVG("Effective Royalty Rate"), 4) AS avg_rate
FROM federal_sales_clean
GROUP BY "Land Category"
ORDER BY avg_rate DESC;

-- ========================================
-- Yearly comparison of sales value vs. royalty revenue
-- Purpose: Compare gross sales and net royalties to evaluate revenue efficiency over time
-- ========================================
SELECT "Calendar Year",
       ROUND(SUM("Sales Value")) AS total_sales,
       ROUND(SUM("Royalty Value Less Allowances (RVLA)")) AS total_royalties
FROM federal_sales_clean
GROUP BY "Calendar Year"
ORDER BY "Calendar Year";

-- ========================================
-- Commodity ranking by sales volume and sales value
-- Question: Which commodities generated the most volume and value in the market?
-- ========================================
SELECT "Commodity", 
       ROUND(SUM("Sales Volume")) AS total_volume,
       ROUND(SUM("Sales Value")) AS total_sales
FROM federal_sales_clean
GROUP BY "Commodity"
ORDER BY total_sales DESC;

-- ========================================
-- Distribution of effective royalty rates (binned)
-- Purpose: Understand how common different royalty rates are across the dataset
-- ========================================
SELECT ROUND("Effective Royalty Rate", 2) AS rate_bucket,
       COUNT(*) AS occurrences
FROM federal_sales_clean
GROUP BY rate_bucket
ORDER BY rate_bucket;

-- ========================================
-- Royalty per gas unit by land category
-- Question: Which land categories yield the highest royalties per MMBtu of gas?
-- ========================================
SELECT "Land Category",
       ROUND(SUM("Royalty Value Less Allowances (RVLA)") / NULLIF(SUM("Gas MMBtu Volume"), 0), 4) AS royalty_per_mmbtu
FROM federal_sales_clean
GROUP BY "Land Category"
ORDER BY royalty_per_mmbtu DESC;

-- ========================================
-- Top 5 regions by average effective royalty rate
-- Question: Where are the royalty rates highest on average?
-- ========================================
SELECT "State/Offshore Region",
       ROUND(AVG("Effective Royalty Rate"), 4) AS avg_royalty_rate
FROM federal_sales_clean
GROUP BY "State/Offshore Region"
ORDER BY avg_royalty_rate DESC
LIMIT 5;

-- ========================================
-- Years with highest transportation cost impact on royalties
-- Question: In which years did transportation costs significantly reduce royalty returns?
-- ========================================
SELECT "Calendar Year",
       ROUND(SUM("Transportation Allowances (TA)"), 2) AS total_transport_cost,
       ROUND(SUM("Royalty Value Less Allowances (RVLA)"), 2) AS total_royalties,
       ROUND(SUM("Transportation Allowances (TA)") / NULLIF(SUM("Royalty Value Less Allowances (RVLA)"), 0) * 100, 2) AS transport_to_royalty_pct
FROM federal_sales_clean
GROUP BY "Calendar Year"
ORDER BY transport_to_royalty_pct DESC
LIMIT 3;

-- ========================================
-- Comprehensive yearly summary with YoY change and cumulative totals
-- Purpose: Provide a full financial overview by year, including trends and growth rates
-- ========================================
WITH year_summary AS (
    SELECT 
        "Calendar Year" AS year,
        SUM("Royalty Value Less Allowances (RVLA)") AS total_royalties,
        SUM("Sales Value") AS total_sales,
        AVG("Effective Royalty Rate") AS avg_royalty_rate
    FROM federal_sales_clean
    GROUP BY "Calendar Year"
)

SELECT 
    year,
    ROUND(total_royalties) AS total_royalties,
    ROUND(total_sales) AS total_sales,
    ROUND(avg_royalty_rate, 4) AS avg_royalty_rate,
    LAG(total_royalties) OVER (ORDER BY year) AS previous_year_royalties,
    ROUND(
        (total_royalties - LAG(total_royalties) OVER (ORDER BY year)) 
        / NULLIF(LAG(total_royalties) OVER (ORDER BY year), 0) * 100, 
        2
    ) AS yoy_percent_change,
    SUM(total_royalties) OVER (ORDER BY year) AS cumulative_royalties
FROM year_summary
ORDER BY year;

-- ========================================
-- Executive summary: top 5 highest royalty-generating regions
-- Purpose: Identify the regions contributing the most to federal royalty revenue
-- ========================================
SELECT "State/Offshore Region",
       ROUND(SUM("Royalty Value Less Allowances (RVLA)")) AS total_royalties,
       ROUND(AVG("Effective Royalty Rate"), 4) AS avg_rate
FROM federal_sales_clean
GROUP BY "State/Offshore Region"
ORDER BY total_royalties DESC
LIMIT 5;