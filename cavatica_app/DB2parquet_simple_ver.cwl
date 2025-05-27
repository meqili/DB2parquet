cwlVersion: v1.2
class: CommandLineTool
label: DB2parquet_simple
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
  - entryname: DB2parquet_simple_ver.py
    entry:
      $include: ../scripts/DB2parquet_simple_ver.py

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
- id: output_dir_name
  type: string
  inputBinding:
    prefix: -O
    position: 1
    shellQuote: false
- id: field_separator
  type:
    type: enum
    name: separator_type
    symbols:
      - comma
      - tab
      - semicolon
      - pipe
  doc: Field separator in the input text file
  inputBinding:
    prefix: -S
    position: 1
    shellQuote: false
- id: tar_ouput
  type: boolean
  doc: "if you want to generate tar.gz files for this database"
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
  type: Directory
  outputBinding:
    glob: $(inputs.output_dir_name)
    loadListing: deep_listing
- id: tarred_database
  type: File
  outputBinding:
    glob: "*tar.gz"

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
    DB2parquet_simple_ver.py
  shellQuote: false
- position: 10
  valueFrom: >
    ${ return inputs.tar_ouput ? '&& tar -czvf ' + inputs.output_dir_name + '.tar.gz ' + inputs.output_dir_name : ''; }
  shellQuote: false