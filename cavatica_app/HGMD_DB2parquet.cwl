cwlVersion: v1.2
class: CommandLineTool
label: HGMD_DB2parquet
$namespaces:
  sbg: https://sevenbridges.com

requirements:
- class: ShellCommandRequirement
- class: LoadListingRequirement
- class: DockerRequirement
  dockerPull: pgc-images.sbgenomics.com/d3b-bixu/pyspark:3.1.2
- class: InlineJavascriptRequirement
- class: InitialWorkDirRequirement
  listing:
  - entryname: HGMD_DB2parquet.py
    entry:
      $include: ../scripts/HGMD_DB2parquet.py

inputs:
- id: input_tarred_hgmd_file
  type: File?
- id: hgmd_version
  type: string
  inputBinding:
    prefix: --hgmd_version
    position: 5
    shellQuote: false
- id: spark_driver_mem
  type: int?
  default: 20
- id: sql_broadcastTimeout
  type: double?
  default: 36000
- id: spark_executor_mem
  type: int?
  default: 30
- id: executor_cores
  type: int?
  default: 8
- id: driver_maxresultsize
  type: int?
  default: 10


outputs:
- id: gene_lite_output
  type: Directory
  outputBinding:
    glob: "*gene_lite"
    loadListing: deep_listing
- id: gene_sorted_output
  type: Directory
  outputBinding:
    glob: "*gene_sorted"
    loadListing: deep_listing
- id: variant_output
  type: Directory
  outputBinding:
    glob: "*variant"
    loadListing: deep_listing

baseCommand:
- tar
- -xvf
arguments:
- position: 1
  valueFrom: |-
    $(inputs.input_tarred_hgmd_file.path)
  shellQuote: false
- position: 2
  valueFrom: |-
    && spark-submit \
    --packages io.projectglow:glow-spark3_2.12:1.1.2  --conf spark.hadoop.io.compression.codecs=io.projectglow.sql.util.BGZFCodec \
    --conf spark.sql.broadcastTimeout=$(inputs.sql_broadcastTimeout) \
    --conf spark.driver.maxResultSize=$(inputs.driver_maxresultsize)G \
    --driver-memory $(inputs.spark_driver_mem)G  \
    --executor-memory $(inputs.spark_executor_mem)G --executor-cores $(inputs.executor_cores) \
    HGMD_DB2parquet.py
  shellQuote: false
