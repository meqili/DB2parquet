cwlVersion: v1.2
class: CommandLineTool
label: DB2parquet
$namespaces:
  sbg: https://sevenbridges.com

requirements:
- class: ShellCommandRequirement
- class: LoadListingRequirement
- class: DockerRequirement
  dockerPull: pgc-images.sbgenomics.com/d3b-bixu/pyspark:3.1.2
- class: InlineJavascriptRequirement

inputs:
- id: input_file
  type: File?
  inputBinding:
    prefix: -I
    position: 1
    shellQuote: false
- id: input_dir
  type: Directory?
  inputBinding:
    prefix: -D
    position: 1
    shellQuote: false
- id: python_script
  type: File
- id: output_dir_name
  type: string
  inputBinding:
    prefix: -O
    position: 1
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
- id: output
  type: Directory?
  outputBinding:
    glob: $(inputs.output_dir_name)
    loadListing: deep_listing

baseCommand:
- spark-submit
arguments:
- prefix: ''
  position: 0
  valueFrom: |-
    --packages io.projectglow:glow-spark3_2.12:1.1.2  --conf spark.hadoop.io.compression.codecs=io.projectglow.sql.util.BGZFCodec \
    --conf spark.sql.broadcastTimeout=$(inputs.sql_broadcastTimeout) \
    --conf spark.driver.maxResultSize=$(inputs.driver_maxresultsize)G \
    --driver-memory $(inputs.spark_driver_mem)G  \
    --executor-memory $(inputs.spark_executor_mem)G --executor-cores $(inputs.executor_cores) \
    $(inputs.python_script.path)
  shellQuote: false
