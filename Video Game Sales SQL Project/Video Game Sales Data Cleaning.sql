CREATE SCHEMA video_games;

USE video_games;

CREATE TABLE video_game_sales (
    `title` VARCHAR(150),
    `console` VARCHAR(10),
    `genre` VARCHAR(20),
    `publisher` VARCHAR(50),
    `developer` VARCHAR(70),
    `critic_score` FLOAT,
    `total_sales` FLOAT,
    `na_sales` FLOAT,
    `jp_sales` FLOAT,
    `pal_sales` FLOAT,
    `other_sales` FLOAT,
    `release_date` VARCHAR(15),
    `last_update` VARCHAR(15)
);

LOAD DATA LOCAL INFILE "C:/Users/silas/Downloads/Video+Game+Sales/vgchartz2024.csv" INTO TABLE video_game_sales FIELDS TERMINATED BY ',' IGNORE 1 LINES;

SELECT 
    *
FROM
    video_game_sales
LIMIT 5;

-- Create a copy of dataset to use for analysis
CREATE TABLE sales_copy LIKE video_game_sales;

INSERT sales_copy
SELECT *
FROM video_game_sales;

-- Data Cleaning
WITH duplicate_cte AS (
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY title, console, genre, publisher, developer, critic_score, total_sales, 
na_sales, jp_sales, pal_sales, other_sales, release_date, last_update) AS row_num
FROM sales_copy)
SELECT *
FROM duplicate_cte
WHERE row_num > 1;

CREATE TABLE `sales_copy_2` (
    `title` VARCHAR(150) DEFAULT NULL,
    `console` VARCHAR(10) DEFAULT NULL,
    `genre` VARCHAR(20) DEFAULT NULL,
    `publisher` VARCHAR(50) DEFAULT NULL,
    `developer` VARCHAR(70) DEFAULT NULL,
    `critic_score` FLOAT DEFAULT NULL,
    `total_sales` FLOAT DEFAULT NULL,
    `na_sales` FLOAT DEFAULT NULL,
    `jp_sales` FLOAT DEFAULT NULL,
    `pal_sales` FLOAT DEFAULT NULL,
    `other_sales` FLOAT DEFAULT NULL,
    `release_date` VARCHAR(15) DEFAULT NULL,
    `last_update` VARCHAR(15) DEFAULT NULL,
    row_num INT
)  ENGINE=INNODB DEFAULT CHARSET=UTF8MB4 COLLATE = UTF8MB4_0900_AI_CI;

INSERT INTO sales_copy_2
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY title, console, genre, publisher, developer, critic_score, total_sales, 
na_sales, jp_sales, pal_sales, other_sales, release_date, last_update) AS row_num
FROM sales_copy;

DELETE FROM sales_copy_2 
WHERE
    row_num > 1;

SELECT 
    *
FROM
    sales_copy_2
WHERE
    row_num > 1;

-- Standardising Data
SELECT DISTINCT
    title
FROM
    sales_copy_2
ORDER BY title;

-- Some records have the console in the genre column and the genre in the publisher column.
-- Grouping consoles and genres with only a few records into other.
SELECT 
    *
FROM
    sales_copy_2
WHERE
    genre IN ('3DS' , 'All',
        'DC',
        'DS',
        'GB',
        'GBA',
        'GC',
        'GEN',
        'Linux',
        'MSD',
        'N64',
        'NES',
        'NS',
        'OSX',
        'PC',
        'PCE',
        'PS',
        'PSV',
        'PS2',
        'PS3',
        'PS4',
        'PS5',
        'PSP',
        'DS',
        'PSN',
        'SAT',
        'SNES',
        'Wii',
        'WiiU',
        'WS',
        'X360',
        'XOne',
        'XB',
        'XBL',
        'XS');

UPDATE sales_copy_2 
SET 
    console = genre,
    genre = publisher
WHERE
    genre IN ('3DS' , 
        'All',
        'DC',
        'DS',
        'GB',
        'GBA',
        'GC',
        'GEN',
        'Linux',
        'MSD',
        'N64',
        'NES',
        'NS',
        'OSX',
        'PC',
        'PCE',
        'PS',
        'PSV',
        'PS2',
        'PS3',
        'PS4',
        'PS5',
        'PSP',
        'DS',
        'PSN',
        'SAT',
        'SNES',
        'Wii',
        'WiiU',
        'WS',
        'X360',
        'XOne',
        'XB',
        'XBL',
        'XS');

CREATE TEMPORARY TABLE miscellaneous_genres AS 
SELECT genre
FROM sales_copy_2
GROUP BY genre
HAVING COUNT(*) <= 10;

UPDATE sales_copy_2 
SET 
    genre = 'Other'
WHERE
    genre IN (SELECT 
            genre
        FROM
            miscellaneous_genres);

SELECT 
    console, COUNT(*)
FROM
    sales_copy_2
GROUP BY console;

CREATE TEMPORARY TABLE miscellaneous_consoles AS 
SELECT console
FROM sales_copy_2
GROUP BY console
HAVING COUNT(*) <= 75;

UPDATE sales_copy_2 
SET 
    console = 'Other'
WHERE
    console IN (SELECT 
            console
        FROM
            miscellaneous_consoles);

SELECT 
    *
FROM
    sales_copy_2
WHERE
    console = 'All';

SELECT 
    SUM(total_sales)
FROM
    sales_copy_2
WHERE console = "3DS";

CREATE TEMPORARY TABLE console_sales (
SELECT console, SUM(total_sales) AS total_sales
FROM sales_copy_2
GROUP BY console);

UPDATE sales_copy_2
SET console = "Other"
WHERE total_sales IN (SELECT total_sales
FROM console_sales
WHERE total_sales <= 101);

SELECT 
    publisher, COUNT(*)
FROM
    sales_copy_2
GROUP BY publisher
HAVING COUNT(*) > 15;

-- Changing records with publishers which are consoles or genres to unknown publisher.
CREATE TEMPORARY TABLE publisher_genres (
SELECT publisher, COUNT(*)
FROM sales_copy_2
GROUP BY publisher
HAVING publisher IN (SELECT DISTINCT genre FROM sales_copy_2));

SELECT 
    *
FROM
    sales_copy_2
WHERE
    publisher IN (SELECT 
            publisher
        FROM
            publisher_genres);

UPDATE sales_copy_2 
SET 
    publisher = 'Unknown'
WHERE
    publisher IN (SELECT 
            publisher
        FROM
            publisher_genres);

CREATE TEMPORARY TABLE publisher_consoles (
SELECT publisher, COUNT(*)
FROM sales_copy_2
GROUP BY publisher
HAVING publisher IN (SELECT DISTINCT console FROM sales_copy_2));

SELECT 
    *
FROM
    sales_copy_2
WHERE
    publisher IN (SELECT 
            publisher
        FROM
            publisher_consoles);

UPDATE sales_copy_2 
SET 
    publisher = 'Unknown'
WHERE
    publisher IN (SELECT 
            publisher
        FROM
            publisher_consoles);

SELECT 
    publisher, COUNT(*)
FROM
    sales_copy_2
GROUP BY publisher
ORDER BY publisher;

CREATE TEMPORARY TABLE other_publishers (
SELECT publisher, COUNT(*) AS count
FROM sales_copy_2
GROUP BY publisher);

UPDATE sales_copy_2 
SET 
    publisher = 'Other'
WHERE
    publisher IN (SELECT 
            publisher
        FROM
            other_publishers
        WHERE
            count < 21);

SELECT 
    developer, COUNT(*)
FROM
    sales_copy_2
GROUP BY developer
HAVING COUNT(*) > 20
ORDER BY developer;

CREATE TEMPORARY TABLE other_developers (
SELECT developer, COUNT(*) AS count
FROM sales_copy_2
GROUP BY developer
HAVING count < 21);

UPDATE sales_copy_2 
SET 
    developer = 'Other'
WHERE
    developer IN (SELECT 
            developer
        FROM
            other_developers
        WHERE
            count < 21);

-- Critic score is supposed to be between 1 and 10
SELECT 
    critic_score, COUNT(*)
FROM
    sales_copy_2
GROUP BY critic_score
HAVING critic_score < 1 OR critic_score > 10
ORDER BY critic_score;

UPDATE sales_copy_2 
SET 
    critic_score = NULL
WHERE
    critic_Score < 1 OR critic_score > 10;

-- Finding titles with total sales that are different than the sum of their components.
SELECT DISTINCT
    total_sales,
    ROUND(COALESCE(na_sales, 0) + COALESCE(jp_sales, 0) + COALESCE(pal_sales, 0) + COALESCE(other_sales, 0),
            2)
FROM
    sales_copy_2
WHERE
    ROUND(COALESCE(na_sales, 0) + COALESCE(jp_sales, 0) + COALESCE(pal_sales, 0) + COALESCE(other_sales, 0),
            2) != total_sales;

UPDATE sales_copy_2 
SET 
    total_sales = ROUND(COALESCE(na_sales, 0) + COALESCE(jp_sales, 0) + COALESCE(pal_sales, 0) + COALESCE(other_sales, 0),
            2)
WHERE
    ROUND(COALESCE(na_sales, 0) + COALESCE(jp_sales, 0) + COALESCE(pal_sales, 0) + COALESCE(other_sales, 0),
            2) != total_sales;

SELECT DISTINCT
    other_sales
FROM
    sales_copy_2
ORDER BY other_sales;

SELECT 
    release_date, COUNT(*)
FROM
    sales_copy_2
GROUP BY release_date
HAVING release_date NOT LIKE '%/%/%';

UPDATE sales_copy_2 
SET 
    release_date = NULL
WHERE
    release_date NOT LIKE '%/%/%';

UPDATE sales_copy_2 
SET 
    last_update = NULL
WHERE
    last_update NOT LIKE '%/%/%';

SELECT 
    STR_TO_DATE(release_date, '%d/%m/%Y')
FROM
    sales_copy_2;

UPDATE sales_copy_2 
SET 
    release_date = STR_TO_DATE(release_date, '%d/%m/%Y');

UPDATE sales_copy_2 
SET 
    last_update = STR_TO_DATE(last_update, '%d/%m/%Y');

SELECT 
    *
FROM
    sales_copy_2
LIMIT 5;

-- Missing Values
SELECT 
    title
FROM
    sales_copy_2
WHERE
    title IS NULL OR TRIM(title) = '';

SELECT 
    console
FROM
    sales_copy_2
WHERE
    console IS NULL OR TRIM(console) = '';

SELECT 
    genre
FROM
    sales_copy_2
WHERE
    genre IS NULL OR TRIM(genre) = '';

SELECT 
    publisher
FROM
    sales_copy_2
WHERE
    publisher IS NULL
        OR TRIM(publisher) = '';

SELECT 
    developer
FROM
    sales_copy_2
WHERE
    developer IS NULL
        OR TRIM(developer) = '';

SELECT 
    critic_score
FROM
    sales_copy_2
WHERE
    critic_score IS NULL
        OR TRIM(critic_score) = '';

SELECT 
    total_sales
FROM
    sales_copy_2
WHERE
    total_sales IS NULL
        OR TRIM(total_sales) = '';

SELECT 
    na_sales
FROM
    sales_copy_2
WHERE
    na_sales IS NULL OR TRIM(na_sales) = '';

SELECT 
    jp_sales
FROM
    sales_copy_2
WHERE
    jp_sales IS NULL OR TRIM(jp_sales) = '';

SELECT 
    pal_sales
FROM
    sales_copy_2
WHERE
    pal_sales IS NULL
        OR TRIM(pal_sales) = '';

SELECT 
    other_sales
FROM
    sales_copy_2
WHERE
    other_sales IS NULL
        OR TRIM(other_sales) = '';

SELECT 
    release_date
FROM
    sales_copy_2
WHERE
    release_date IS NULL
        OR TRIM(release_date) = '';

SELECT 
    release_date, critic_score
FROM
    sales_copy_2
WHERE
    release_date IS NULL
        OR TRIM(release_date) = ''
        AND critic_score IS NULL;
-- All records with null release_dates have null critic_scores.

SELECT 
    *
FROM
    sales_copy_2
WHERE
    last_update IS NULL;

ALTER TABLE sales_copy_2
DROP COLUMN row_num;

WITH sales_sums AS (
SELECT SUM(na_sales) AS na_sum, SUM(jp_sales) AS jp_sum, SUM(pal_sales) AS pal_sum, SUM(other_sales) AS other_sum
FROM sales_copy_2)
SELECT MAX(na_sum, jp_sum, pal_sum, other_sum)
FROM sales_sums;
