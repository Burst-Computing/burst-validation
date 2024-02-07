# Terasort classic APP
In this file, we can find the detailed list of steps to execute the terasort classic application. 

The classic version is formed by two steps: called "map" and "reduce". Actually, they are two fases separated 
by a shuffle (all-to-all) exchanges. Object storage is used as communication channel for intermediate exchanges.

## Precompile the code
Terasort source code is burdensome. Then, we will follow the approach of precompiling the code in the client side.

> If your code is already precompiled, please go to [Step 9](#execute-your-experiment).

1. Access to the terasort-map directory. 

```bash
   $BASE_DIR=$(pwd) # Root of this repo
   cd $BASE_DIR/ow_functions/terasort-map
```

2. Ensure that this directory have the following structure: 

```bash
   |- Cargo.toml
   |- src
      |- lib.rs
```

3. Execute the next command:

```bash
   zip -r - * | docker run -i manriurv/rust-burst -debug -compile main > ../terasort-map.zip
```
4. Ensure the existence of the `terasort-map.zip` file in the `ow_functions` directory.
5. Access to the terasort-reduce directory. 

```bash
   cd $BASE_DIR/ow_functions/terasort-reduce
```

6. Ensure that this directory have the following structure: 

```bash
   |- Cargo.toml
   |- src
      |- lib.rs
```

7. Execute the next command:

```bash
   zip -r - * | docker run -i manriurv/rust-burst -debug -compile main > ../terasort-reduce.zip
```
8. Ensure the existence of the `terasort-reduce.zip` file in the `ow_functions` directory.
## Execute your experiment
9. Navigate to the base dir:

```bash
   cd $BASE_DIR
```
10. (Optional) Show usage help of the script:


```
    PYTHONPATH=. python3 ow_apps/terasort_classic/main.py --help
    usage: main.py [-h] --endpoint ENDPOINT --partitions PARTITIONS --bucket BUCKET --key KEY
    
    options:
      -h, --help            show this help message and exit
      --endpoint ENDPOINT   Endpoint of the S3 service in which the terasort file is stored
      --partitions PARTITIONS
                            Number of partitions to sort the file into
      --bucket BUCKET       Terasort bucket name
      --key KEY             Terasort object key
```

11. Execute the burst terasort application (the following is just an example):

```bash
    PYTHONPATH=. python3 ow_apps/terasort_burst/main.py --endpoint http://minio:9000 \
    --partitions 10 --bucket terasort --key terasort-1g 
```

