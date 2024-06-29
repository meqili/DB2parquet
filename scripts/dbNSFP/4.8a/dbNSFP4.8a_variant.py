import argparse
from argparse import RawTextHelpFormatter
import pyspark
import glow
from pyspark.sql.functions import col, udf, expr, concat, lit
from pyspark.sql.types import StringType, LongType
import re

# Argument parser setup
parser = argparse.ArgumentParser(
    description='Script for gene-based variant filtering. \n\
    MUST BE RUN WITH spark-submit. For example: \n\
    spark-submit --driver-memory 10G Gene_based_variant_filtering.py',
    formatter_class=RawTextHelpFormatter)

parser.add_argument('-I', '--input_file', required=False,
                    help='A text file to be converted to a parquet file')
parser.add_argument('-D', '--input_dir', required=False,
                    help='Directory containing files')
parser.add_argument('-O', '--output_name', required=True,
                    help='Name of the output parquet file')
args = parser.parse_args()

# Create spark session
spark = (
    pyspark.sql.SparkSession.builder.appName("DB2parquet")
    .config('spark.sql.parquet.enableVectorizedReader', 'false')
    .getOrCreate()
)

# Register glow functions
spark = glow.register(spark)

# Parameter configuration
input_database = args.input_file if args.input_file else args.input_dir
dir_path = args.output_name

# Read input data
# The inputs for this script is a folder that contains all file named dbNSFP4.8a_variant.chrxxxx.gz
# dbNSFP4.8a_variant.chrM.gz is excluded 
dbNSFP_variant = spark.read.options(inferSchema=True, sep="\t", header=True, nullValue=".") \
    .csv(input_database)

# Replace characters in column names, and those characters are incompatible in pyspark
cols = [re.sub(r'(^_|_$)', '', c.replace("(", "[").replace(" ", "_").replace(")", "]")) for c in dbNSFP_variant.columns]

# Rename columns
dbNSFP_variant = dbNSFP_variant.toDF(*cols) \
    .withColumnRenamed("#chr", "chromosome") \
    .withColumn("pos[1-based]", col("pos[1-based]").cast(LongType())) \
    .withColumnRenamed("pos[1-based]", "start") \
    .withColumnRenamed("ref", "reference") \
    .withColumnRenamed("alt", "alternate") \
    .drop("#chr", "pos[1-based]", "ref", "alt")

# Save columns to keep in the future
columns_to_keep = dbNSFP_variant.columns

# Function for general prediction scores
def determine_prediction_scores(vep, prediction_score):
    if vep is None or prediction_score is None:
        return None

    vep_list = vep.split(';')
    prediction_list = prediction_score.split(';')

    if(len(vep_list) > len(prediction_list)):
        damage_count = sum(prediction_list.count(x) for x in ['D', 'P', 'H', 'M', 'U'])
        nondamage_count = sum(prediction_list.count(x) for x in ['T', 'B', 'N', 'L', 'A'])
    else:
        relevant_preds = [s for v, s in zip(vep_list, prediction_list) if v == 'YES']
        damage_count = sum(relevant_preds.count(x) for x in ['D', 'P', 'H', 'M', 'U'])
        nondamage_count = sum(relevant_preds.count(x) for x in ['T', 'B', 'N', 'L', 'A'])

    if damage_count > nondamage_count:
        return 'Damage'
    elif nondamage_count > damage_count:
        return 'NonDamage'
    elif damage_count == nondamage_count and damage_count > 0:
        return 'Damage'
    else:
        return None

determine_prediction_scores_udf = udf(determine_prediction_scores, StringType())

# Function for Aloft_pred
# Aloft_pred has prediction score that conflicts other scores
def determine_Aloft_pred(vep, prediction_score):
    if vep is None or prediction_score is None:
        return None

    vep_list = vep.split(';')
    prediction_list = prediction_score.split(';')

    relevant_preds = [s for v, s in zip(vep_list, prediction_list) if v == 'YES']
    
    damage_count = sum(1 for x in relevant_preds if 'Dominant' in x or 'Recessive' in x)
    nondamage_count = sum(1 for x in relevant_preds if 'Tolerant' in x and "|" not in x)

    if damage_count > nondamage_count:
        return 'Damage'
    elif nondamage_count > damage_count:
        return 'NonDamage'
    elif damage_count == nondamage_count and damage_count > 0:
        return 'Damage'
    else:
        return None

determine_Aloft_pred_udf = udf(determine_Aloft_pred, StringType())

# Generate Filtered_ columns for each prediction score
# 1. skip MutationTaster_pred
# And Aloft_pred has its own function
prediction_columns = [c for c in dbNSFP_variant.columns if '_pred' in c and c not in ["MutationTaster_pred", "Aloft_pred"]]
for col_name in prediction_columns:
    new_col_name = f"Filtered_{col_name}"
    dbNSFP_variant = dbNSFP_variant.withColumn(new_col_name, determine_prediction_scores_udf(col("VEP_canonical"), col(col_name)))

dbNSFP_variant = dbNSFP_variant.withColumn("Filtered_Aloft_pred", determine_Aloft_pred_udf(col("VEP_canonical"), col("Aloft_pred")))

# Count Total_NonNull_Count and Total_Damage_Count values
Filtered_prediction_columns = [c for c in dbNSFP_variant.columns if '_pred' in c and 'Filtered_' in c]
escaped_columns = [f"`{col}`" for col in Filtered_prediction_columns]

non_null_expr = " + ".join([f"IF({col} IS NOT NULL, 1, 0)" for col in escaped_columns])
damage_expr = " + ".join([f"IF({col} = 'Damage', 1, 0)" for col in escaped_columns])

dbNSFP_variant = dbNSFP_variant.withColumn("Total_NonNull_Count", expr(non_null_expr)) \
                               .withColumn("Total_Damage_Count", expr(damage_expr))

# Generate DamagePredCount column
dbNSFP_variant = dbNSFP_variant.withColumn("DamagePredCount", concat(col("Total_Damage_Count"), lit("_"), col("Total_NonNull_Count")))

# Write to parquet files
dbNSFP_variant.select([c for c in columns_to_keep] + ["DamagePredCount"]) \
              .write.mode("overwrite") \
              .partitionBy("chromosome") \
              .parquet(dir_path)
