# Terasort Burst APP
In this file, we can find the detailed list of steps to execute the terasort burst application.

## Precompile the code
Terasort source code is burdensome. Then, we will follow the approach of precompiling the code in the client side.

> If your code is already precompiled, please go to [Step 5](#execute-your-experiment).

1. Access to the terasort directory. 

```bash
   $BASE_DIR=$(pwd) # Root of this repo
   cd $BASE_DIR/ow_functions/terasort-burst
```

2. Ensure that this directory have the following structure: 

```bash
   |- Cargo.toml
   |- src
      |- lib.rs
```

3. Execute the next command:

```bash
   zip -r - * | docker run -i manriurv/rust-burst -debug -compile main > ../terasort-burst.zip
```
4. Ensure the existence of the `terasort-burst.zip` file in the `ow_functions` directory.

## Execute your experiment

5. Navigate to the base dir:

```bash
   cd $BASE_DIR
```
6. (Optional) Show usage help of the script:


```
    PYTHONPATH=. python3 ow_apps/terasort_burst/main.py --help
    usage: main.py [-h] --ow-host OW_HOST --ow-port OW_PORT
               [--runtime-memory RUNTIME_MEMORY] [--custom-image CUSTOM_IMAGE]
               --ts-endpoint TS_ENDPOINT --partitions PARTITIONS --bucket
               BUCKET --key KEY [--granularity GRANULARITY] [--join JOIN]
               --backend {rabbitmq,redis,redis_stream,redis_list,s3}
               [--chunk-size CHUNK_SIZE]
    options:
      -h, --help            show this help message and exit
      --ow-host OW_HOST     Openwhisk host
      --ow-port OW_PORT     Openwhisk port
      --runtime-memory RUNTIME_MEMORY
                            Memory to allocate to the runtime (in MB)
      --custom-image CUSTOM_IMAGE
                            Tag of the docker custom image to use
      --ts-endpoint TS_ENDPOINT
                            Endpoint of the S3 service in which the terasort file is stored
      --partitions PARTITIONS
                            Number of partitions to sort the file into
      --bucket BUCKET       Terasort bucket name
      --key KEY             Terasort object key
      --granularity GRANULARITY
                            Granularity of burst workers
      --join JOIN           Join burst workers in same invoker
      --backend {rabbitmq,redis,redis_stream,redis_list,s3}
                            Burst communication backend
      --chunk-size CHUNK_SIZE
                            Chunk size for burst messages (in KB)

```

7. Execute the burst terasort application following the specs above. For example:

```bash
    PYTHONPATH=. python3 ow_apps/terasort_burst/main.py --ow-host "172.17.0.1" --ow-port 3233 \
    --runtime-memory 4096 --ts-endpoint http://minio:9000 --partitions 10 --bucket terasort \
    --key 10g --granularity 1 --join False --backend rabbitmq --chunk-size 32
```

