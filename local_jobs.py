from pyspark.sql import SparkSession, Window, DataFrame
from pyspark.sql.functions import col, dense_rank, when, datediff, current_date, round
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType, BooleanType, DecimalType


def get_spark(no_cores: int = 1) -> SparkSession:
    return SparkSession.builder.appName('corporate_uk').master(f'local[{no_cores}]').getOrCreate()

def read_companies(spark: SparkSession, fpath: str) -> DataFrame:
    schema = StructType([
        StructField('company_number', StringType(), True),
        StructField('company_type', StringType(), True),
        StructField('office_address', StringType(), True),
        StructField('incorporation_date', DateType(), True),
        StructField('jurisdiction', StringType(), True),
        StructField('company_status', StringType(), True),
        StructField('account_type', StringType(), True),
        StructField('company_name', StringType(), True),
        StructField('sic_codes', StringType(), True),
        StructField('date_of_cessation', DateType(), True),
        StructField('next_accounts_overdue', BooleanType(), True),
        StructField('confirmation_statement_overdue', BooleanType(), True),
        StructField('owners', IntegerType(), True),
        StructField('officers', IntegerType(), True),
        StructField('average_number_employees_during_period', IntegerType(), True),
        StructField('current_assets', DoubleType(), True),
        StructField('last_accounts_period_end', DateType(), True),
        StructField('company_url', StringType(), True),
    ])
    companies = spark.read.option('delimiter', ';').option('header', 'true').schema(schema).csv(fpath)\
                .withColumn('account_type', when(col('account_type') == 'Null', None).otherwise(col('account_type')))
    return companies

def read_sic_codes(spark: SparkSession, fpath: str) -> DataFrame:
    schema = StructType([
        StructField('company_number', StringType(), True),
        StructField('sic_code', IntegerType(), True),
        StructField('sic_description', StringType(), True),
        StructField('sic_section', StringType(), True),
        StructField('sic_division', StringType(), True),
        StructField('company_url', StringType(), True),
    ])
    sic_codes = spark.read.option('delimiter', ';').option('header', 'true').schema(schema).csv(fpath)
    return sic_codes

def read_filings(spark: SparkSession, fpath: str) -> DataFrame:
    schema = StructType([
        StructField('company_number', StringType(), False),
        StructField('date', DateType(), True),
        StructField('category', StringType(), True),
        StructField('pages', IntegerType(), True),
        StructField('description', StringType(), True),
    ])
    filings = spark.read.option('delimiter', ';').option('header', 'true').schema(schema).csv(fpath)
    return filings

def read_owners(spark: SparkSession, fpath: str) -> DataFrame:
    schema = StructType([
        StructField('company_number', StringType(), True),
        StructField('name', StringType(), True),
        StructField('kind', StringType(), True),
        StructField('officer_role', StringType(), True),
        StructField('occupation', StringType(), True),
        StructField('date', DateType(), True),
        StructField('is_owner', BooleanType(), True),
        StructField('country_of_residence', StringType(), True),
        StructField('nationality', StringType(), True),
        StructField('company_country', StringType(), True),
        StructField('person_id', IntegerType(), True),
        StructField('person_url', StringType(), True),
    ])
    owners = spark.read.option('delimiter', ';').option('header', 'true').schema(schema).csv(fpath)
    return owners

def account_types_job(companies: DataFrame) -> DataFrame:
    # Which account type is most common in each jurisdiction?
    w = Window.partitionBy(['jurisdiction']).orderBy(col('count').desc())
    account_types = companies.filter(col('jurisdiction').isNotNull() & col('account_type').isNotNull())\
            .groupBy(['jurisdiction', 'account_type'])\
            .count()\
            .withColumn('rnk', dense_rank().over(w))\
            .where(col('rnk') == 1)\
            .drop(col('rnk'))\
            .sort(col('jurisdiction'))
    return account_types

def company_ages_job(companies: DataFrame) -> DataFrame:
    # Which company types are the oldest on average?
    company_ages = companies.withColumn('age', datediff(current_date(), col('incorporation_date')) / 365.25)\
            .select([
                'company_type', 
                'age'])\
            .groupBy('company_type')\
            .avg()\
            .withColumn('average_age', round(col('avg(age)'), 2))\
            .drop(col('avg(age)'))\
            .sort(col('average_age'))
    return company_ages

def sic_current_assets_job(companies: DataFrame, sic_codes: DataFrame) -> DataFrame:
    # Which SIC codes have the highest current assets on average?
    sic_assets = companies.join(sic_codes, 'company_number', how='left')\
        .select([
            companies.company_number, 
            sic_codes.sic_code, 
            sic_codes.sic_description,
            companies.current_assets
        ])\
        .groupBy('sic_description')\
        .avg('current_assets')\
        .withColumn('avg(current_assets)', col('avg(current_assets)').cast(DecimalType(38, 10)))\
        .sort('avg(current_assets)', ascending=False)
    return sic_assets

def number_of_owners_job(owners: DataFrame, companies: DataFrame) -> DataFrame:
    # Which companies have the highest number of owners?
    number_of_owners = owners.groupBy('company_number')\
        .count()\
        .join(companies, 'company_number', how='left')\
        .select([
            'company_number',
            'company_name',
            'count'
        ])\
        .sort('count', ascending=False)
    return number_of_owners

def pages_of_filings_job(filings: DataFrame, companies: DataFrame) -> DataFrame:
    # Which companies have the most pages of filings?
    number_of_pages = filings.groupBy('company_number')\
        .sum('pages')\
        .join(companies, 'company_number', how='left')\
        .select([
            'company_number',
            'company_name',
            'sum(pages)'
        ])\
        .sort('sum(pages)', ascending=False)
    return number_of_pages

def write_dataframe(df: DataFrame, fpath: str) -> None:
    df.write.mode('OVERWRITE').csv(fpath, header=True)

if __name__ == "__main__":
    spark = get_spark(no_cores=4)
    companies = read_companies(spark, './corporate_uk/companies.csv')
    sic_codes = read_sic_codes(spark, './corporate_uk/companies_sic_codes.csv')
    filings = read_filings(spark, './corporate_uk/filings.csv')
    owners = read_owners(spark, './corporate_uk/officers_and_owners.csv')
    company_ages = company_ages_job(companies)
    account_types = account_types_job(companies)
    number_of_owners = number_of_owners_job(owners, companies)
    pages_of_filings = pages_of_filings_job(filings, companies)
    current_assets = sic_current_assets_job(companies, sic_codes)
    write_dataframe(company_ages, './output/company_ages.csv')
    write_dataframe(account_types, './output/account_types.csv')
    write_dataframe(number_of_owners, './output/no_of_owners.csv')
    write_dataframe(pages_of_filings, './output/pages_of_filings.csv')
    write_dataframe(current_assets, './output/current_assets.csv')