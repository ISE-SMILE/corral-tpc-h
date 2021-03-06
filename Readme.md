# Examples

Corral++ offers many ways to execute and run jobs, this repo implements a set of the TPC-H queries necessary to run the benchmark. This folder contains a set of examples to run the benchmark with different Corral++ deployment modes.

## Running the benchmark

First you'll need to generate the test data. The test data is generated by the [TPC-H benchmark](https://www.tpc.org/tpch/). For ease of use, you can use the docker image in the `./dbgen` folder, it generates the data in the `./data` folder inside the container. Use volume mounting or `docker cp` to copy the data to the host.
Depending on how you want to perform the benchmark you'll need to upload the data to S3, Minio or another supported storage system.

Note that the format of the data needs to be in the schema: `<table_name>/table_<part>.tbl`. 

Once you have the data, you can run the benchmark using config files for repeatability.

To run the benchmark, you can use the following command: `go run main.go -config=<config_file>`.

### Config
```json
{
    "query": 0,
    "backend": "local",
    "experiment": "1",
    "endpoint": "test",
    "undeploy": false,
    "randomize": true,
    "validation": false,
    "cache": "redis"
}
```

|     Name     | Description                                                                                                                     | Default |
|:------------:|---------------------------------------------------------------------------------------------------------------------------------|:-------:|
|   `query`    | The tpc-h query to run, 0-21.                                                                                                   |   `0`   |
|  `backend`   | The backend to use, local, lambda, whisk.                                                                                       | `local` |
| `experiment` | The folder/bucket the experiment data is in. We assume that this is a number equivalent to the sf factor of the tpch experiment |   `1`   |
|  `endpoint`  | The data endpoint, e.g., a folder or a s3-url                                                                                   | `test`  |
|  `undeploy`  | Whether to undeploy all resources after the query run                                                                           | `false` |
| `randomize`  | Whether to randomize the query parameter                                                                                        | `true`  |
| `validation` | Whether to validate the result, only possible if randomized=false                                                               | `false` |
|   `cache`    | The cache to use, redis, efs, etc.                                                                                              |   ``    |

You can add all corral config parameter to this config file, and they will be forwarded to corral.

### Results
By default, the query results are stored in the `<endpoint>/output` folder.

### Measurements
By default, the benchmark automatically logs measurements of each job stored in the `./runs` folder if it exists.

Measurements follow the following naming pattern: `<backend>_<confighash>_<randomseed>_<mode>_<backend>_<experiment>_<query>_<quaryvalues>.csv`.

Each log file contains the following columns:

|  Name   | Description                                          |  Domain   |
|:-------:|------------------------------------------------------|:---------:|
|   JId   | Job ID, combination of `<jobNumber>_<phase>_<binID>` |  String   |
|   CId   | Runtime ID, unique per runtime enviroment            | String(8) |
|   HId   | Unique Host ID, depends on Backend                   |  String   |
|   RId   | Runtime ID                                           |  String   |
| CStart  | First Starttime in UnixNano                          |    Int    |
| EStart  | Start of Execution Time                              |    Int    |
|  EEnd   | End of Execution Time                                |    Int    |
|  Read   | #bytes that were read                                |    Int    |
| Written | #bytes that were written                             |    Int    |

