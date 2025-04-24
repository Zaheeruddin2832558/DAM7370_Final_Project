CREATE SCHEMA gold;
------------------------------
-- CREATE VIEW: credit_risk_raw
------------------------------
CREATE VIEW gold.credit_risk_raw
AS
SELECT
    *
FROM
    OPENROWSET(
        BULK 'https://creditriskdataset.blob.core.windows.net/silver/credit_risk_cleaned/',
        FORMAT = 'PARQUET'
    ) AS [data];
-- ------------------------------
-- CREATE VIEW: customer_demographics
-- ------------------------------
CREATE VIEW gold.customer_demographics
AS
SELECT
    person_age,
    person_income,
    person_home_ownership,
    person_emp_length,
    cb_person_cred_hist_length,
    cb_person_default_on_file
FROM
    OPENROWSET(
        BULK 'https://creditriskdataset.blob.core.windows.net/silver/credit_risk_cleaned/',
        FORMAT = 'PARQUET'
    ) AS QUER1;
GO

-- ------------------------------
-- CREATE VIEW: loan_details
-- ------------------------------
CREATE VIEW gold.loan_details
AS
SELECT
    loan_intent,
    loan_grade,
    loan_amnt,
    loan_int_rate,
    loan_percent_income,
    loan_status
FROM
    OPENROWSET(
        BULK 'https://creditriskdataset.blob.core.windows.net/silver/credit_risk_cleaned/',
        FORMAT = 'PARQUET'
    ) AS QUER1;
GO

-- ------------------------------
-- CREATE VIEW: loan_defaults_only
-- ------------------------------
CREATE VIEW gold.loan_defaults_only
AS
SELECT
    *
FROM
    OPENROWSET(
        BULK 'https://creditriskdataset.blob.core.windows.net/silver/credit_risk_cleaned/',
        FORMAT = 'PARQUET'
    ) AS QUER1
WHERE
    loan_status = 1;
GO

-- ------------------------------
-- CREATE VIEW: high_risk_loans
-- ------------------------------
CREATE VIEW gold.high_risk_loans
AS
SELECT
    loan_intent,
    loan_grade,
    loan_amnt,
    loan_int_rate,
    loan_status
FROM
    OPENROWSET(
        BULK 'https://creditriskdataset.blob.core.windows.net/silver/credit_risk_cleaned/',
        FORMAT = 'PARQUET'
    ) AS QUER1
WHERE
    loan_int_rate > 15 OR loan_grade IN ('E', 'F', 'G');
GO

-- ------------------------------
-- CREATE VIEW: default_rate_by_loan_intent
-- ------------------------------
CREATE VIEW gold.default_rate_by_loan_intent
AS
SELECT
    loan_intent,
    COUNT(*) AS total_loans,
    SUM(CASE WHEN loan_status = 1 THEN 1 ELSE 0 END) AS defaulted_loans,
    ROUND(SUM(CASE WHEN loan_status = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS default_rate_percentage
FROM
    OPENROWSET(
        BULK 'https://creditriskdataset.blob.core.windows.net/silver/credit_risk_cleaned/',
        FORMAT = 'PARQUET'
    ) AS QUER1
GROUP BY loan_intent;
GO

-- ------------------------------
-- CREATE VIEW: avg_interest_by_grade
-- ------------------------------
CREATE VIEW gold.avg_interest_by_grade
AS
SELECT
    loan_grade,
    ROUND(AVG(loan_int_rate), 2) AS avg_interest_rate
FROM
    OPENROWSET(
        BULK 'https://creditriskdataset.blob.core.windows.net/silver/credit_risk_cleaned/',
        FORMAT = 'PARQUET'
    ) AS QUER1
GROUP BY loan_grade;
GO

-- ------------------------------
-- CREATE VIEW: income_vs_default
-- ------------------------------
CREATE VIEW gold.income_vs_default
AS
SELECT
    person_income,
    loan_status
FROM
    OPENROWSET(
        BULK 'https://creditriskdataset.blob.core.windows.net/silver/credit_risk_cleaned/',
        FORMAT = 'PARQUET'
    ) AS QUER1;
GO

-- ------------------------------
-- CREATE VIEW: home_ownership_distribution
-- ------------------------------
CREATE VIEW gold.home_ownership_distribution
AS
SELECT
    person_home_ownership,
    COUNT(*) AS customer_count
FROM
    OPENROWSET(
        BULK 'https://creditriskdataset.blob.core.windows.net/silver/credit_risk_cleaned/',
        FORMAT = 'PARQUET'
    ) AS QUER1
GROUP BY person_home_ownership;
GO

