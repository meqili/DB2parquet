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
  type: File
  inputBinding:
    prefix: -I
    position: 0
    shellQuote: false
- id: python_script
  type: File
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
- id: output_dir_name
  type: string

outputs:
- id: output
  type: Directory?
  outputBinding:
    glob: $(inputs.input_file.nameroot)
    loadListing: deep_listing

baseCommand:
- spark-submit
arguments:
- prefix: ''
  position: 0
  valueFrom: |-
    --packages io.projectglow:glow-spark3_2.12:1.1.2  --conf spark.hadoop.io.compression.codecs=io.projectglow.sql.util.BGZFCodec --conf spark.sql.broadcastTimeout=$(inputs.sql_broadcastTimeout) --driver-memory $(inputs.spark_driver_mem)G  --executor-memory $(inputs.spark_executor_mem)G --executor-cores  $(inputs.executor_cores) $(inputs.python_script.path) -O $(inputs.output_dir_name)
  shellQuote: false
id: |-
  https://cavatica-api.sbgenomics.com/v2/apps/yiran/variant-workbench-testing/db2parquet/5/raw/
sbg:appVersion:
- v1.2
sbg:content_hash: a52a8c1f3cf5e01be4a755d797ef6db83fba114b5d41be492da2c341f1fbd1708
sbg:contributors:
- qqlii44
sbg:createdBy: qqlii44
sbg:createdOn: 1701722902
sbg:id: yiran/variant-workbench-testing/db2parquet/5
sbg:image_url:
sbg:latestRevision: 5
sbg:modifiedBy: qqlii44
sbg:modifiedOn: 1706738703
sbg:project: yiran/variant-workbench-testing
sbg:projectName: Variant WorkBench testing
sbg:publisher: sbg
sbg:revision: 5
sbg:revisionNotes: customized output dir name
sbg:revisionsInfo:
- sbg:modifiedBy: qqlii44
  sbg:modifiedOn: 1701722902
  sbg:revision: 0
  sbg:revisionNotes:
- sbg:modifiedBy: qqlii44
  sbg:modifiedOn: 1701723292
  sbg:revision: 1
  sbg:revisionNotes: ''
- sbg:modifiedBy: qqlii44
  sbg:modifiedOn: 1701723413
  sbg:revision: 2
  sbg:revisionNotes: added output port
- sbg:modifiedBy: qqlii44
  sbg:modifiedOn: 1701724605
  sbg:revision: 3
  sbg:revisionNotes: ''
- sbg:modifiedBy: qqlii44
  sbg:modifiedOn: 1701724920
  sbg:revision: 4
  sbg:revisionNotes: updated output name
- sbg:modifiedBy: qqlii44
  sbg:modifiedOn: 1706738703
  sbg:revision: 5
  sbg:revisionNotes: customized output dir name
sbg:sbgMaintained: false
sbg:validationErrors: []
sbg:workflowLanguage: CWL
