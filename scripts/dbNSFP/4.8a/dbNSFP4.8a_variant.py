import argparse
from argparse import RawTextHelpFormatter
import pyspark
import glow

parser = argparse.ArgumentParser(
    description='Script of gene based variant filtering. \n\
    MUST BE RUN WITH spark-submit. For example: \n\
    spark-submit --driver-memory 10G Gene_based_variant_filtering.py',
    formatter_class=RawTextHelpFormatter)

parser = argparse.ArgumentParser()
parser.add_argument('-I', '--input_file', required=False,
                    help='a text file that to be converted to a partquet file')
parser.add_argument('-D', '--input_dir', required=False,
                    help='Directory containing files')
parser.add_argument('-O', '--output_name', required=True,
                    help='name of the output parquert file')
args = parser.parse_args()

# Create spark session
spark = (
    pyspark.sql.SparkSession.builder.appName("DB2parquet")
    .config('spark.sql.parquet.enableVectorizedReader', 'false')
    .getOrCreate()
    )
# Register so that glow functions like read vcf work with spark. Must be run in spark shell or in context described in help
spark = glow.register(spark)

# parameter configuration
if args.input_file:
    input_database = args.input_file
else:
    input_database = args.input_dir
dir_path = args.output_name


## mian 
from pyspark.sql.functions import col, udf, expr, concat, lit
from pyspark.sql.types import StringType
from pyspark.sql.types import LongType
import re

## input for dbNSFP_variant is a directory which contains all files with name like dbNSFP4.8a_variant.chrxxx.gz 
## except file dbNSFP4.8a_variant.chrM.gz
dbNSFP_variant = spark.read.options(inferSchema=True,sep="\t",header=True,nullValue=".") \
            .csv(input_database)

## replacing characters in column names that are not readble in pyspark
cols=[re.sub(r'(^_|_$)','',c.replace("(", "[").replace(" ", "_").replace(")", "]")) for c in dbNSFP_variant.columns]

## intial rename 
dbNSFP_variant = dbNSFP_variant.toDF(*cols) \
    .withColumnRenamed("#chr", "chromosome") \
    .withColumn("pos[1-based]", col("pos[1-based]").cast(LongType())) \
    .withColumnRenamed("pos[1-based]", "start") \
    .withColumnRenamed("ref", "reference") \
    .withColumnRenamed("alt", "alternate") \
    .drop("#chr", "pos[1-based]", "ref", "alt")

## intial columns to keep in the future
columns_to_keep = dbNSFP_variant.columns

## function for all prediction scores column other than MutationTaster_pred
def determine_prediction_scores(vep, prediction_score):
    if vep is None or prediction_score is None:
        return None
    
    vep_list = vep.split(';')
    prediction_list = prediction_score.split(';')
    
    # Extract SIFT_pred values corresponding to YES in VEP_canonical
    relevant_preds = [s for v, s in zip(vep_list, prediction_list) if v == 'YES']
    
    # Count occurrences of D and T
    damage_count = relevant_preds.count('D') + relevant_preds.count('P') + relevant_preds.count('H') + relevant_preds.count('M')
    nondamage_count = relevant_preds.count('T') + relevant_preds.count('B') + relevant_preds.count('N') + relevant_preds.count('U') + relevant_preds.count('L')
    
    # Determine the value based on the counts
    if damage_count > nondamage_count:
        return 'Damage'
    elif nondamage_count > damage_count:
        return 'NonDamage'
    elif damage_count == nondamage_count and damage_count > 0:
        return 'Damage'
    else:
        return None

determine_prediction_scores_udf = udf(determine_prediction_scores, StringType())

## since column MutationTaster_pred has conflicated values with other prediction scores. 
## this function is specific for that
def determine_MutationTaster_pred(vep, prediction_score):
    if vep is None or prediction_score is None:
        return None
    
    vep_list = vep.split(';')
    prediction_list = prediction_score.split(';')
    
    # Extract SIFT_pred values corresponding to YES in VEP_canonical
    relevant_preds = [s for v, s in zip(vep_list, prediction_list) if v == 'YES']
    
    # Count occurrences of D and T
    damage_count = relevant_preds.count('D') + relevant_preds.count('A')
    nondamage_count = relevant_preds.count('N') + relevant_preds.count('P')
    
    # Determine the value based on the counts
    if damage_count > nondamage_count:
        return 'Damage'
    elif nondamage_count > damage_count:
        return 'NonDamage'
    elif damage_count == nondamage_count and damage_count > 0:
        return 'Damage'
    else:
        return None

determine_MutationTaster_pred_udf = udf(determine_MutationTaster_pred, StringType())


## generated Filtered_ columns for each prediction scores
## skip AlphaMissense and ALoft
prediction_columns = [c for c in dbNSFP_variant.columns if '_pred' in c and c != "AlphaMissense_pred" and c != "Aloft_pred" and c != "MutationTaster_pred"]

# Apply the UDF to each prediction column and create a new column
for col_name in prediction_columns:
    new_col_name = f"Filtered_{col_name}"
    dbNSFP_variant = dbNSFP_variant.withColumn(new_col_name, determine_prediction_scores_udf(col("VEP_canonical"), col(col_name)))

dbNSFP_variant = dbNSFP_variant.withColumn("Filtered_MutationTaster_pred", determine_prediction_scores_udf(col("VEP_canonical"), col("MutationTaster_pred")))

## count Total_NonNull_Count and Total_Damage_Count valyes
# Properly escape column names using backticks
Filtered_prediction_columns = [c for c in dbNSFP_variant.columns if '_pred' in c and 'Filtered_' in c]
escaped_columns = [f"`{col}`" for col in Filtered_prediction_columns]

# Create expressions to count non-null values and 'Damage' values
non_null_expr = " + ".join([f"IF({col} IS NOT NULL, 1, 0)" for col in escaped_columns])
damage_expr = " + ".join([f"IF({col} = 'Damage', 1, 0)" for col in escaped_columns])

# Add new columns for the counts
dbNSFP_variant = dbNSFP_variant.withColumn("Total_NonNull_Count", expr(non_null_expr)) \
               .withColumn("Total_Damage_Count", expr(damage_expr))


## generated column DamagePredCount
dbNSFP_variant = dbNSFP_variant.withColumn("DamagePredCount", concat(col("Total_Damage_Count"), lit("_"), col("Total_NonNull_Count")))

## write dbNSFP_variant to parquet files
dbNSFP_variant.select([c for c in columns_to_keep] + ["DamagePredCount"]) \
    .write.mode("overwrite") \
    .partitionBy("chromosome") \
    .parquet(dir_path)
