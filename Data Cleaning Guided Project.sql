-- Data Cleaning

SELECT *
FROM layoffs;

-- 1. Remove Duplicates
-- 2. Standardise Data
-- 3. Missing values
-- 4. Remove Irrelevant Columns

CREATE TABLE layoffs_staging
LIKE layoffs;

SELECT * 
FROM layoffs_staging;

INSERT layoffs_staging
SELECT * 
FROM layoffs;

WITH duplicate_cte AS(
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, industry, total_laid_off, percentage_laid_off, 'date, stage, country, funds_raised_millions') AS row_num
FROM layoffs_staging)
SELECT * 
FROM duplicate_cte
WHERE row_num > 1;

SELECT *
FROM layoffs_staging
WHERE company = 'Casper';

CREATE TABLE `layoffs_staging_2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` bigint DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

INSERT INTO layoffs_staging_2
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, industry, total_laid_off, percentage_laid_off, 'date, stage, country, funds_raised_millions') AS row_num
FROM layoffs_staging;

DELETE
FROM layoffs_staging_2
WHERE row_num > 1;

SELECT *
FROM layoffs_staging_2;

-- Standardising Data

SELECT company, TRIM(company)
FROM layoffs_staging_2;

UPDATE layoffs_staging_2
SET company = TRIM(company);

SELECT DISTINCT industry
FROM layoffs_staging_2
ORDER BY 1;

SELECT *
FROM layoffs_staging_2
WHERE industry LIKE 'Crypto%';

UPDATE layoffs_staging_2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

SELECT DISTINCT country, TRIM(TRAILING "." FROM country)
FROM layoffs_staging_2
ORDER  BY 1;

UPDATE layoffs_staging_2
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%';

SELECT `date`
FROM layoffs_staging_2;

UPDATE layoffs_staging_2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

ALTER TABLE layoffs_staging_2
MODIFY COLUMN `date` DATE;

SELECT industry
FROM layoffs_staging_2
WHERE industry IS nULL
OR industry = '';

SELECT *
FROM layoffs_staging_2
WHERE industry IS NULL
OR industry = '';

UPDATE layoffs_staging_2
SET industry = NULL
WHERE industry = '';

SELECT *
FROM layoffs_staging_2  AS t1
JOIN layoffs_staging_2 AS t2
ON t1.company = t2.company
AND t1.location = t2.location
WHERE (t1.industry IS NULL OR t1.industry = '')
AND t2.industry IS NOT NULL;

UPDATE layoffs_staging_2 AS t1
JOIN layoffs_staging_2 AS t2
ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE (t1.industry IS NULL OR t1.industry = '') 
AND t2.industry IS NOT NULL;

-- Remove any columns or rows

SELECT *
FROM layoffs_staging_2
WHERE total_laid_off IS NULL 
AND percentage_laid_off IS NULL;

DELETE
FROM layoffs_staging_2
WHERE total_laid_off IS NULL 
AND percentage_laid_off IS NULL;

SELECT *
FROM layoffs_staging_2;

ALTER TABLE layoffs_staging_2
DROP COLUMN row_num;