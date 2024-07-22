-- Exploratory Data Analysis

SELECT *
FROM layoffs_staging_2;

SELECT MAX(total_laid_off), MAX(percentage_laid_off)
FROM layoffs_staging_2;

SELECT *
FROM layoffs_staging_2
WHERE percentage_laid_off = 1
ORDER BY funds_raised_millions DESC;

SELECT company, SUM(total_laid_off)
FROM layoffs_staging_2
GROUP BY company
ORDER BY 2 DESC;

SELECT MIN(`date`), MAX(`date`)
FROM layoffs_staging_2;

SELECT industry, SUM(total_laid_off)
FROM layoffs_staging_2
GROUP BY industry
ORDER BY 2 DESC;

SELECT country, SUM(total_laid_off)
FROM layoffs_staging_2
GROUP BY country
ORDER BY 2 DESC;

SELECT YEAR(`date`), SUM(total_laid_off)
FROM layoffs_staging_2
GROUP BY YEAR(`date`)
ORDER BY 1 DESC;

SELECT stage, SUM(total_laid_off)
FROM layoffs_staging_2
GROUP BY stage
ORDER BY 2 DESC;

SELECT SUBSTRING(`date`, 1, 7) AS `Month`, SUM(total_laid_off) 
FROM layoffs_staging_2
WHERE SUBSTRING(`date`, 1, 7) IS NOT NULL
GROUP BY `Month`
ORDER BY 1 DESC;

WITH Rolling_Total AS (
SELECT SUBSTRING(`date`, 1, 7) AS `Month`, SUM(total_laid_off) AS total_laid_off
FROM layoffs_staging_2
WHERE SUBSTRING(`date`, 1, 7) IS NOT NULL
GROUP BY `Month`
ORDER BY 1 DESC)
SELECT `Month`, total_laid_off, 
SUM(total_laid_off) OVER(ORDER BY `Month`)  AS rolling_total
FROM Rolling_Total;

SELECT company, SUM(total_laid_off)
FROM layoffs_staging_2
GROUP BY company
ORDER BY 2 DESC;

SELECT company, YEAR(`date`), SUM(total_laid_off)
FROM layoffs_staging_2
GROUP BY company, YEAR(`date`)
ORDER BY 3 DESC;

WITH Company_Year (company, years, total_laid_off) AS (
SELECT company, YEAR(`date`), SUM(total_laid_off)
FROM layoffs_staging_2
GROUP BY company, YEAR(`date`)
), Company_Year_Rank AS 
(SELECT *, DENSE_RANK() OVER(PARTITION BY years ORDER BY total_laid_off DESC) AS Ranking
FROM Company_Year
WHERE years IS NOT NULL)

SELECT * FROM Company_Year_Rank
WHERE Ranking <= 5
ORDER BY Ranking ASC;