{
    "class": "CommandLineTool",
    "cwlVersion": "v1.2",
    "$namespaces": {
        "sbg": "https://sevenbridges.com"
    },
    "baseCommand": [
        "spark-submit"
    ],
    "inputs": [
        {
            "id": "input_file",
            "type": "File",
            "inputBinding": {
                "prefix": "-I",
                "shellQuote": false,
                "position": 0
            }
        },
        {
            "id": "python_script",
            "type": "File"
        },
        {
            "id": "spark_driver_mem",
            "type": "int?",
            "default": 48
        },
        {
            "id": "sql_broadcastTimeout",
            "type": "double?",
            "default": 36000
        },
        {
            "id": "spark_executor_mem",
            "type": "int?",
            "default": 34
        },
        {
            "id": "executor_cores",
            "type": "int?",
            "default": 5
        }
    ],
    "outputs": [
        {
            "id": "output",
            "type": "Directory?",
            "outputBinding": {
                "glob": "$(inputs.input_file.nameroot)",
                "loadListing": "deep_listing"
            }
        }
    ],
    "label": "DB2parquet",
    "arguments": [
        {
            "prefix": "",
            "shellQuote": false,
            "position": 0,
            "valueFrom": "--packages io.projectglow:glow-spark3_2.12:1.1.2  --conf spark.hadoop.io.compression.codecs=io.projectglow.sql.util.BGZFCodec --conf spark.sql.broadcastTimeout=$(inputs.sql_broadcastTimeout) --driver-memory $(inputs.spark_driver_mem)G  --executor-memory $(inputs.spark_executor_mem)G --executor-cores  $(inputs.executor_cores) $(inputs.python_script.path) -O $(inputs.input_file.nameroot)"
        }
    ],
    "requirements": [
        {
            "class": "ShellCommandRequirement"
        },
        {
            "class": "LoadListingRequirement"
        },
        {
            "class": "DockerRequirement",
            "dockerPull": "pgc-images.sbgenomics.com/d3b-bixu/pyspark:3.1.2"
        },
        {
            "class": "InlineJavascriptRequirement"
        }
    ],
    "sbg:projectName": "Variant WorkBench testing",
    "sbg:revisionsInfo": [
        {
            "sbg:revision": 0,
            "sbg:modifiedBy": "qqlii44",
            "sbg:modifiedOn": 1701722902,
            "sbg:revisionNotes": null
        },
        {
            "sbg:revision": 1,
            "sbg:modifiedBy": "qqlii44",
            "sbg:modifiedOn": 1701723292,
            "sbg:revisionNotes": ""
        },
        {
            "sbg:revision": 2,
            "sbg:modifiedBy": "qqlii44",
            "sbg:modifiedOn": 1701723413,
            "sbg:revisionNotes": "added output port"
        },
        {
            "sbg:revision": 3,
            "sbg:modifiedBy": "qqlii44",
            "sbg:modifiedOn": 1701724605,
            "sbg:revisionNotes": ""
        },
        {
            "sbg:revision": 4,
            "sbg:modifiedBy": "qqlii44",
            "sbg:modifiedOn": 1701724920,
            "sbg:revisionNotes": "updated output name"
        }
    ],
    "sbg:image_url": null,
    "sbg:appVersion": [
        "v1.2"
    ],
    "id": "https://cavatica-api.sbgenomics.com/v2/apps/yiran/variant-workbench-testing/db2parquet/4/raw/",
    "sbg:id": "yiran/variant-workbench-testing/db2parquet/4",
    "sbg:revision": 4,
    "sbg:revisionNotes": "updated output name",
    "sbg:modifiedOn": 1701724920,
    "sbg:modifiedBy": "qqlii44",
    "sbg:createdOn": 1701722902,
    "sbg:createdBy": "qqlii44",
    "sbg:project": "yiran/variant-workbench-testing",
    "sbg:sbgMaintained": false,
    "sbg:validationErrors": [],
    "sbg:contributors": [
        "qqlii44"
    ],
    "sbg:latestRevision": 4,
    "sbg:publisher": "sbg",
    "sbg:content_hash": "a53198bc6bb2b7256ed003e08c79f29131f6b3ed8907b383c59896086af7379c2",
    "sbg:workflowLanguage": "CWL"
}
