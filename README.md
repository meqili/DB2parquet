# DB2parquet
[Here](https://www.notion.so/d3b/f958b89d2fc343a7bcfa41e4dfaa0a90?v=951ee808eb714a7fbf3239397941f648) is the notion page that documents all databases available in VWB.

# Basic format of databases in VWB
  1. these columns’ names are identical across tables, chromosome, start, and end. If applicable, these columns’ names need to be matched, reference, and alternate.
  2. The column chromosome is without leading “chr”
  3. The column start is 1-based

# Running command
`spark-submit
--packages io.projectglow:glow-spark3_2.12:1.1.2 --conf spark.hadoop.io.compression.codecs=io.projectglow.sql.util.BGZFCodec  --conf spark.sql.broadcastTimeout=2.88986851674124  --conf spark.driver.maxResultSize=3G  --driver-memory 5G  --executor-memory 7G --executor-cores 6  /path/to/python_script.ext
 -D /path/to/input_dir  -I /path/to/input_file.ext -O output_dir_name-string-value`


## CAVATICA APP: HGMD_DB2paruqet
This app is specifically designed for importing the HGMD database to parquet files. It requires two inputs:

- **Tarred HGMD File**: This file should contain three files with specific naming conventions:
  - The filenames must start with `hg38` followed by the HGMD version, such as `HGMD2024Q1`, and the corresponding affix for each file.
  - Example filenames: `hg38_HGMD2024Q1_gene_lite.VWB.txt`, `hg38_HGMD2024Q1_gene_sorted.VWB.txt`, and `hg38_HGMD2024Q1_variant.VWB.txt`.
  - It's crucial that the filenames strictly adhere to these requirements.
  - This tar file is generated from Yiran's [workflow](https://github.com/Yiran-Guo/Annovar-database-update#updating-hgmd-2020q1).
- **HGMD Version**: Specify the version of HGMD you are using, such as `HGMD2024Q11`.
