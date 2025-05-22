SELECT* 
FROM layoffs;
-- 1) Remove Duplicates
-- 2) Standardize the data
-- 3) Null values or blank value
-- 4) Remove any columns
#1 removing duplicates
# Copying all the data into a new table so we don't create undesirable changes in the og table
CREATE TABLE layoff_staging
LIKE layoffs; 
SELECT*
FROM layoff_staging;
INSERT layoff_staging
SELECT*
FROM layoffs;
WITH duplicate_cte as(
SELECT*,
ROW_NUMBER() OVER(
PARTITION BY company,industry,total_laid_off,percentage_laid_off,`date`,funds_raised_millions,location) AS row_num
FROM layoff_staging
)
DELETE
FROM duplicate_cte
WHERE row_num>1;# cannot do this because row_num isn't technically a row 
CREATE TABLE `layoff_staging2` ( #creating another table which has a row called row_num
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
SELECT*
FROM layoff_staging2;
INSERT INTO layoff_staging2
SELECT*,
ROW_NUMBER() OVER(
PARTITION BY company,industry,total_laid_off,percentage_laid_off,`date`,funds_raised_millions,location) AS row_num
FROM layoff_staging;
SET SQL_SAFE_UPDATES = 0; #Turning safe update off in order to delete rows
DELETE
FROM layoff_staging2 #deleting from the new table yay! duplicates removed
WHERE row_num>1;
SET SQL_SAFE_UPDATES = 1; #Turning safe update back on
# Standardizing the data
SELECT*
FROM layoff_staging2
ORDER BY 1;
SELECT company, TRIM(company)
FROM layoff_staging2;
UPDATE layoff_staging2
SET company=TRIM(company);
SELECT*
FROM layoff_staging2
WHERE industry LIKE 'Crypto%';
UPDATE layoff_staging2
SET industry= 'Crypto'
WHERE industry LIKE 'Crypto%';
SELECT DISTINCT industry
FROM layoff_staging2;
UPDATE layoff_staging2
SET industry=TRIM(industry);
SELECT DISTINCT country
FROM layoff_staging2
ORDER BY 1;
UPDATE layoff_staging2
SET country=TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%';
#changing date from text datatype to date
SELECT `date`
FROM layoff_staging2;
UPDATE layoff_staging2
SET `date`= STR_TO_DATE(`date`,'%m/%d/%Y');

ALTER TABLE layoff_staging2
MODIFY COLUMN `date` DATE;
#Removing null values

UPDATE layoff_staging2
SET industry=null
WHERE industry='';
SELECT*
FROM layoff_staging2
WHERE industry IS NULL
OR industry='';
#now we will attempt to populate the industry column
#we will join the table with itself giving the condition of where the industry is null and where it is not null
SELECT t1.industry,t2.industry,t1.company,t2.company
FROM layoff_staging2 t1
JOIN layoff_staging t2
ON t1.company=t2.company
AND t1.location=t2.location
WHERE t1.industry IS NULL OR t1.industry=''
AND t2.industry IS NOT NULL;
UPDATE layoff_staging2 t1
JOIN layoff_staging2 t2
	ON t1.company=t2.company
SET t1.industry=t2.industry
WHERE t1.industry IS NULL 
AND t2.industry IS NOT NULL;
#now we will remove the columns where there is no lay off
DELETE
FROM layoff_staging2
WHERE total_laid_off IS NULL
AND
percentage_laid_off IS NULL;
#now we will drop the column row_num as we dont need it anymore
ALTER TABLE layoff_staging2
DROP row_num;
-- WE HAVE A CLEAN DATA!!!!!




















