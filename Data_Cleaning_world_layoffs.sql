USE world_layoffs;

SELECT *
FROM layoffs_raw;

-- Remove Duplicates if any
-- Standardize data set
-- Remove NULLS & BLANKS
-- Take out unnecessary rows / columns

CREATE TABLE layoff_staging
LIKE layoffs_raw;

SELECT *
FROM layoff_staging;

INSERT INTO layoff_staging
SELECT *
FROM layoffs_raw;

SELECT *
FROM layoff_staging;

WITH duplicate_cte AS
(
SELECT *, 
ROW_NUMBER() OVER(PARTITION BY company, location, industry, total_laid_off, 
percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoff_staging
)
SELECT *
FROM duplicate_cte
WHERE row_num > 1
;

CREATE TABLE `layoff_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

SELECT *
FROM layoff_staging2;

INSERT INTO layoff_staging2
SELECT *, 
ROW_NUMBER() OVER(PARTITION BY company, location, industry, total_laid_off, 
percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoff_staging;


SELECT *
FROM layoff_staging2
WHERE row_num > 1;


DELETE
FROM layoff_staging2
WHERE row_num > 1;

-- Standardization
SELECT *
FROM layoff_staging2;

SELECT DISTINCT(company), TRIM(company)
FROM layoff_staging2;

UPDATE layoff_staging2
SET company = TRIM(company)
;

SELECT DISTINCT (industry)
FROM layoff_staging2
ORDER BY 1;

UPDATE layoff_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

SELECT `date`,
	STR_TO_DATE(`date`, '%m/%d/%Y')
FROM layoff_staging2
;

UPDATE layoff_staging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

ALTER TABLE layoff_staging2
MODIFY COLUMN `date` DATE;

SELECT DISTINCT(country), TRIM(TRAILING '.' FROM country)
FROM layoff_staging2
ORDER BY 1;

UPDATE layoff_staging2
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%';

-- Removing NULLS and BLANKS
SELECT *
FROM layoff_staging2
;

UPDATE layoff_staging2
SET industry = null
WHERE industry = '';

SELECT *
FROM layoff_staging2
WHERE industry IS NULL;

SELECT t1.industry, t2.industry
FROM layoff_staging2 t1
JOIN layoff_staging2 t2
	ON t1.company = t2.company
    AND t1.location = t2.location
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;


UPDATE layoff_staging2 t1
JOIN layoff_staging2 t2
	ON t1.company = t2.company
    AND t1.location = t2.location
SET t1.industry = t2.industry
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;

DELETE
FROM layoff_staging2
WHERE total_laid_off IS NULL 
AND percentage_laid_off IS NULL;

SELECT *
FROM layoff_staging2
WHERE total_laid_off IS NULL 
AND percentage_laid_off IS NULL;

ALTER TABLE layoff_staging2
DROP COLUMN row_num;

SELECT *
FROM layoff_staging2;