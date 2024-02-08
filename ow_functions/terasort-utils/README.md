# Terasort Burst application

### Setup
1. Setup a new Python environment:
    ```
    $ python3 -m venv venv
    ```

    ```
    $ source venv/bin/activate
    ```

    ```
    $ (venv) pip install -r requirements.txt 
    ```


2. To create an input JSON payload use the ```generate_payload.py``` script. The options are:
    ```
    --partitions PARTITIONS
                            Number of partitions
    --bucket BUCKET       Bucket name
    --key KEY             Object key
    --sort-output-key SORT_OUTPUT_KEY
                            Sort output key
    --sort-column SORT_COLUMN
                            Sort key
    --delimiter DELIMITER
                            Delimiter
    --start-margin START_MARGIN
                            Start margin
    --end-margin END_MARGIN
                            End margin
    --sample-ratio SAMPLE_RATIO
                            Sample ratio
    --sample-fragments SAMPLE_FRAGMENTS
                            Sample fragments
    --max-sample-size MAX_SAMPLE_SIZE
                            Max sample size
    --bound-margin BOUND_MARGIN
                            Bound extraction margin
    --seed SEED           Random seed
    --payload-filename PAYLOAD_FILENAME
                            Payload filename
    --tmp-prefix TMP_PREFIX
                            Prefix for temorary data in S3
    --rabbitmq-uri RABBITMQ_URI
                            RabbitMQ uri for burst indirect communication

    ```
    - Example:

    ```
    $ (venv) python generate_payload.py --partitions 4 --sample-fragments 1 --bucket terasort --key terasort-1g --sort-column 0
    ```