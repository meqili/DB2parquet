import argparse
import os
import pyspark
import glow

def process_file(input_df, output_name, base_name, rename_columns=None, drop_columns=None):
    """Process a single file: clean, rename columns, and save as parquet."""
   
    # Clean column names: replace spaces and special characters
    new_columns = [col.replace(" ", "_").replace(",", "_").replace(";", "_").replace("=", "_") 
                   for col in input_df.columns]
    df = input_df.toDF(*new_columns)
    
    # Rename and drop columns as needed
    if rename_columns:
        for old_name, new_name in rename_columns.items():
            df = df.withColumnRenamed(old_name, new_name)
    
    if drop_columns:
        df = df.drop(*drop_columns)
    
    # Write to Parquet
    output_file = os.path.join(output_name, f"OLIDA_{base_name.lower()}")
    df.coalesce(1).write.mode("overwrite").parquet(output_file)

def main():
    # Argument parser
    parser = argparse.ArgumentParser(
        description='Gene-based variant filtering. \nMUST BE RUN WITH spark-submit.',
        formatter_class=argparse.RawTextHelpFormatter)
    
    parser.add_argument('-I', '--input_file', required=False, help='Single file to be converted to parquet')
    parser.add_argument('-D', '--input_dir', required=False, help='Directory containing text files to be converted')
    parser.add_argument('-O', '--output_name', required=True, help='Output parquet file name')
    
    args = parser.parse_args()
    
    # Initialize Spark session
    spark = pyspark.sql.SparkSession.builder.appName("DB2parquet").getOrCreate()
    spark = glow.register(spark)  # Register glow for VCF functions
    
    # Determine input source
    input_database = args.input_file if args.input_file else args.input_dir
    output_name = args.output_name
    
    # List of files to process
    files = {
        "Combination.tsv": {},
        "Disease.tsv": {},
        "GeneCombination.tsv": {},
        "Reference.tsv": {},
        "SMALLVARIANT.tsv": {
            "rename_columns": {
                "Chromosome": "chromosome", 
                "Genomic_Position_Hg38": "start",
                "Ref_Allele": "reference",
                "Alt_Allele": "alternate"
            },
            "drop_columns": ["Chromosome", "Genomic_Position_Hg38", "Ref_Allele", "Alt_Allele"]
        },
        "Gene.tsv": {
            "rename_columns": {"Chromosome": "chromosome"},
            "drop_columns": ["Chromosome"]
        },
        "CNV.tsv": {
            "rename_columns": {
                "Chromosome": "chromosome", 
                "Genomic_Position_Hg38": "start"
            },
            "drop_columns": ["Chromosome", "Genomic_Position_Hg38"]
        }
    }
    
    # Process each file
    for file, options in files.items():
        file_path = os.path.join(input_database, file)
        df = spark.read.options(inferSchema=True, sep="\t", header=True, nullValue="N.A.").csv(file_path)
        base_name, _ = os.path.splitext(os.path.basename(file_path))
        process_file(df, output_name, base_name, options.get("rename_columns"), options.get("drop_columns"))

if __name__ == "__main__":
    main()
