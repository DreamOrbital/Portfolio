SELECT 
    *
FROM
    sales_copy_2;

SELECT 
    MAX(critic_score),
    MAX(total_sales),
    MAX(na_sales),
    MAX(jp_sales),
    MAX(pal_sales),
    MAX(other_sales)
FROM
    sales_copy_2;

SELECT 
    AVG(critic_score),
    AVG(total_sales),
    AVG(na_sales),
    AVG(jp_sales),
    AVG(pal_sales),
    AVG(other_sales)
FROM
    sales_copy_2;

SELECT 
    STD(critic_score),
    STD(total_sales),
    STD(na_sales),
    STD(jp_sales),
    STD(pal_sales),
    STD(other_sales)
FROM
    sales_copy_2;

SELECT 
    genre, ROUND(SUM(total_sales), 2) AS total_sold
FROM
    sales_copy_2
GROUP BY genre
ORDER BY total_sold DESC;

SELECT 
    genre, ROUND(AVG(critic_score), 2) AS avg_critic_score
FROM
    sales_copy_2
GROUP BY genre
ORDER BY avg_critic_score DESC;

SELECT 
    console, ROUND(SUM(total_sales), 2) AS total_sold
FROM
    sales_copy_2
GROUP BY console
ORDER BY total_sold DESC;

SELECT 
    console, ROUND(AVG(critic_score), 2) AS avg_critic_score
FROM
    sales_copy_2
GROUP BY console
ORDER BY avg_critic_score DESC;

WITH Rolling_Total AS (
SELECT SUBSTRING(release_date, 1, 7) AS `Month`, SUM(total_sales) AS total_sales
FROM sales_copy_2
WHERE SUBSTRING(release_date, 1, 7) IS NOT NULL
GROUP BY `Month`
ORDER BY 1 DESC)
SELECT `Month`, ROUND(total_sales, 2), 
ROUND(SUM(total_sales) OVER(ORDER BY `Month`), 2)  AS rolling_total
FROM Rolling_Total;

WITH Rolling_Average AS (
SELECT SUBSTRING(release_date, 1, 7) AS `Month`, AVG(critic_score) AS critic_score
FROM sales_copy_2
WHERE SUBSTRING(release_date, 1, 7) IS NOT NULL
GROUP BY `Month`
ORDER BY 1 DESC)
SELECT `Month`, ROUND(critic_score, 2), 
ROUND(AVG(critic_score) OVER(ORDER BY `Month`), 2)  AS rolling_average
FROM Rolling_Average;

SELECT 
    genre, YEAR(release_date), ROUND(SUM(total_sales), 2)
FROM
    sales_copy_2
GROUP BY genre , YEAR(release_date)
ORDER BY 3 DESC;

SELECT 
    genre, YEAR(release_date), ROUND(AVG(critic_score), 2)
FROM
    sales_copy_2
GROUP BY genre , YEAR(release_date)
ORDER BY 3 DESC;

SELECT 
    console, YEAR(release_date), ROUND(SUM(total_sales), 2)
FROM
    sales_copy_2
GROUP BY console , YEAR(release_date)
ORDER BY 3 DESC;

SELECT 
    console, YEAR(release_date), ROUND(AVG(critic_score), 2)
FROM
    sales_copy_2
GROUP BY console , YEAR(release_date)
ORDER BY 3 DESC;

WITH genre_year (genre, years, total_sales) AS (
SELECT genre, YEAR(release_date), SUM(total_sales)
FROM sales_copy_2
GROUP BY genre, YEAR(release_date)
), genre_year_rank AS 
(SELECT *, DENSE_RANK() OVER(PARTITION BY years ORDER BY total_sales DESC) AS Ranking
FROM genre_year)

SELECT * FROM genre_year_rank
ORDER BY Ranking ASC;
