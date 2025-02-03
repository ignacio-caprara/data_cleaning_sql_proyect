-- Data Cleaning Project: Demonstrating SQL Skills and Knowledge.

-- Dataset: https://www.kaggle.com/datasets/swaptr/layoffs-2022

-- This project involves cleaning a dataset of layoffs to demonstrate my skills in SQL, 
-- data manipulation, and data cleaning processes.

-- Throughout the project, I will use SQL techniques such as window functions, 
-- string manipulation, date conversion, and data filtering to ensure that the dataset 
-- is clean, consistent, and ready for further analysis or reporting.

-- The final result will be a clean and structured dataset, ready for exploratory data analysis (EDA) and insights generation.

SELECT *
FROM layoffs;

-- Data Cleaning Steps:
-- 1. Check for and remove duplicate entries.
-- 2. Standardize data formatting and fix errors.
-- 3. Analyze null values and determine how to handle them.
-- 4. Remove unnecessary columns and rows.

-- Create a new staging table with the same structure as the original table.
CREATE TABLE layoffs_staging
LIKE layoffs;

-- Copy all data from the original layoffs table into the new staging table.
INSERT INTO layoffs_staging
SELECT *
FROM layoffs;

-- View the data in the staging table.
SELECT *
FROM layoffs_staging;

-- 1. Check for Duplicates and Remove Them:
-- I will use window functions, specifically ROW_NUMBER(), to identify unique rows 
-- and distinguish them from duplicates.

SELECT *,
    ROW_NUMBER() OVER (PARTITION BY company, location, industry, total_laid_off, `date`, 
    stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging;

-- Identify rows that are duplicates based on the fields specified.
WITH duplicates_cte AS (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY company, location, industry, total_laid_off, `date`, 
        stage, country, funds_raised_millions) AS row_num
    FROM layoffs_staging
)
SELECT *
FROM duplicates_cte
WHERE row_num > 1;

-- Create a new table to store the data, including row_num, to facilitate removing duplicates later.
CREATE TABLE `layoffs_staging2` (
  `company` TEXT,
  `location` TEXT,
  `industry` TEXT,
  `total_laid_off` INT DEFAULT NULL,
  `percentage_laid_off` TEXT,
  `date` TEXT,
  `stage` TEXT,
  `country` TEXT,
  `funds_raised_millions` INT DEFAULT NULL,
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Insert data from the staging table into the new table, along with the row number to identify duplicates.
INSERT INTO layoffs_staging2
SELECT *,
    ROW_NUMBER() OVER (PARTITION BY company, location, industry, total_laid_off, `date`, 
    stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging;

-- Identify duplicate rows in the new table.
SELECT * 
FROM layoffs_staging2
WHERE row_num > 1;

-- Delete duplicate rows from the new table.
DELETE
FROM layoffs_staging2
WHERE row_num > 1;

-- Verify the table to ensure duplicates have been removed.
SELECT * 
FROM layoffs_staging2;

-- 2. Standardizing Data:
-- I will use the TRIM function to remove any leading or trailing spaces from string fields,
-- ensuring the data is consistent and clean.

SELECT 
  TRIM(company) AS company,
  TRIM(location) AS location,
  TRIM(industry) AS industry,
  TRIM(stage) AS stage,
  TRIM(country) AS country
FROM layoffs_staging2;

-- Update the table to remove extra spaces from each of the fields.
UPDATE layoffs_staging2
SET 
  company = TRIM(company),
  location = TRIM(location),
  industry = TRIM(industry),
  stage = TRIM(stage),
  country = TRIM(country);

-- Check for distinct values in each column to identify any errors in the data.

SELECT DISTINCT company
FROM layoffs_staging2
ORDER BY company;

SELECT DISTINCT location
FROM layoffs_staging2
ORDER BY location;

-- Typographical errors in some location names have been detected and will be corrected.

UPDATE layoffs_staging2
SET location = CASE
    WHEN location = 'DÃ¼sseldorf' THEN 'Düsseldorf'
    WHEN location = 'FlorianÃ³polis' THEN 'Florianópolis'
    WHEN location = 'MalmÃ¶' THEN 'Malmö'
    ELSE location
END;
  
SELECT DISTINCT industry
FROM layoffs_staging2
ORDER BY industry;

-- The terms "Crypto" and "CryptoCurrency" refer to the same industry and will be standardized.

SELECT *
FROM layoffs_staging2
WHERE industry LIKE 'Crypto%';

UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

SELECT DISTINCT country
FROM layoffs_staging2
ORDER BY country;

-- Two variations of "United States" exist, one with a period at the end. I will correct this inconsistency.

SELECT *
FROM layoffs_staging2
WHERE country LIKE 'United States.%';

UPDATE layoffs_staging2
SET country = 'United States'
WHERE country LIKE 'United States%';

-- Verify that there are no negative values in the "total_laid_off" column.
SELECT DISTINCT total_laid_off
FROM layoffs_staging2
ORDER BY total_laid_off;

-- Verify that there are no negative values in the "percentage_laid_off" column.
SELECT DISTINCT percentage_laid_off
FROM layoffs_staging2
ORDER BY percentage_laid_off;

-- Verify that there are no negative values in the "funds_raised_millions" column.
SELECT DISTINCT funds_raised_millions
FROM layoffs_staging2
ORDER BY funds_raised_millions;

-- Check that all values in the "date" column are valid.
SELECT DISTINCT `date`
FROM layoffs_staging2;

-- Convert the "date" field from a string format to a proper date format.
SELECT `date`, str_to_date(`date`, '%m/%d/%Y')
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET `date` = str_to_date(`date`, '%m/%d/%Y');

ALTER TABLE layoffs_staging2 MODIFY COLUMN `date` DATE;

-- 3. Handling Null Values:
-- Look for null or empty values in the "company" and "location" columns.
SELECT *
FROM layoffs_staging2
WHERE company IS NULL 
OR company = ''
ORDER BY company;

SELECT *
FROM layoffs_staging2
WHERE location IS NULL 
OR location = ''
ORDER BY location;

-- No null or empty values are found in the "company" and "location" columns.

-- In the "industry" column, there are some null or empty values. I will attempt to fill these using existing data.

SELECT *
FROM layoffs_staging2
WHERE industry IS NULL 
OR industry = ''
ORDER BY industry;

-- Check data for specific company (Bally's Interactive) to see if we have additional information for the industry.
SELECT *
FROM layoffs_staging2
WHERE company = 'Bally\'s Interactive';

-- No additional information for this company, so we will leave it as is.

-- Check for another company (Airbnb) and see if we can determine the industry.
SELECT *
FROM layoffs_staging2
WHERE company = 'Airbnb';

-- Additional information indicates the industry is "Travel."

-- Now let's check if there are any remaining null or empty values in the "industry" column.
SELECT *
FROM layoffs_staging2
WHERE industry IS NULL 
OR industry = ''
ORDER BY industry;

-- Set empty strings in the "industry" column to null, as empty values are harder to handle.
UPDATE layoffs_staging2
SET industry = NULL
WHERE industry = '';

-- Populate null "industry" values based on matching company names.
UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;

-- Check for any remaining null or empty fields.
SELECT *
FROM layoffs_staging2
WHERE 
    company = '' OR
    location = '' OR
    industry = '' OR
    total_laid_off = '' OR
    percentage_laid_off = '' OR
    stage = '' OR
    country = '' OR
    funds_raised_millions = '';
    
-- It can be confirmed that there are no longer any empty values, only nulls remain.

-- The null values in "total_laid_off", "percentage_laid_off", and "funds_raised_millions" are normal and don't need to be modified.
-- It's preferable to leave them as null, as it simplifies calculations during the exploratory data analysis (EDA) phase.

-- 4. Removing Unnecessary Columns and Rows:
SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

-- Delete rows where both "total_laid_off" and "percentage_laid_off" are null, as they contain no useful information.
DELETE FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

SELECT * 
FROM layoffs_staging2;

-- Drop the "row_num" column, as it is no longer needed after cleaning the data.
ALTER TABLE layoffs_staging2 DROP COLUMN row_num;

SELECT * 
FROM layoffs_staging2;
