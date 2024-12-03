-- Data cleaning

select *
from layoffs;

-- 0. duplicate the table
-- 1. Remove duplicates
-- 2. standardie the data
-- 3. Null values or blank values
-- 4. Remove any unwanted column

CREATE TABLE Layoffs_staging
like layoffs;

select *
from layoffs_staging;


insert layoffs_staging
select *
from layoffs;

-- 1.

select *,
row_number() over(
PARTITION BY company,location,industry,total_laid_off,percentage_laid_off,'date',stage,country,funds_raised_millions) as row_num
from layoffs_staging;


select *
from layoffs_staging
where company = 'casper';




CREATE TABLE `layoffs_staging2` (
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

select *
from layoffs_staging2;

insert into layoffs_staging2
select *,
row_number() over(
PARTITION BY company,location,industry,total_laid_off,percentage_laid_off,'date',stage,country,funds_raised_millions) as row_num
from layoffs_staging;

select *
from layoffs_staging2
where row_num >1;

DELETE
from layoffs_staging2
where row_num >1;

select *
from layoffs_staging2;

-- standaediing data
select company,trim(company)
from layoffs_staging2;

update layoffs_staging2
set company=trim(company);

select *
from layoffs_staging2
where industry like 'cryp%';

update layoffs_staging2
set industry = 'Crypto'
where industry like 'crypto%';

select distinct industry
from layoffs_staging2;

select distinct country,trim(trailing'.' from country)
from layoffs_staging2
order by 1;

update layoffs_staging2
set country =trim(trailing'.' from country)
where country like 'United States%';

select date,
str_to_date(date, '%m/%d/%Y')
from layoffs_staging2;

update layoffs_staging2
set date = str_to_date(date, '%m/%d/%Y');

select date
from layoffs_staging2;

alter table layoffs_staging2
modify column date DATE;

-- null values

update layoffs_staging2
set industry = null
where industry = '';

SELECT *
FROM layoffs_staging2;

SELECT *
FROM layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;

SELECT  *
FROM layoffs_staging2
where industry is null
or industry = '';

SELECT *
FROM layoffs_staging2
where company like 'bally%';

select *
from layoffs_staging2 t1
join layoffs_staging2 t2
   on t1.company=t2.company
   and t1.location=t2.location
where (t1.industry is null)
and t2.industry is not null  ;

update layoffs_staging2 t1 
join layoffs_staging2 t2
   on t1.company=t2.company
set t1.industry =t2.industry
where (t1.industry is null )
and t2.industry is not null;   

select *
from layoffs_staging2;

SELECT *
FROM layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;


DELETE
FROM layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;

SELECT *
FROM layoffs_staging2;

ALTER TABLE layoffs_staging2
DROP COLUMN row_num;

SELECT *
FROM layoffs_staging2;















