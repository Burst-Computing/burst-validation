# Terasort application

# Generate payload

Example for 128 partitons and local MinIO:

```bash
$ python3 generate_payload.py --partitions 128 --bucket terasort --key terasort-10g --sort-column 0 --seed 42 --s3_region us-east-1 --s3_endpoint http://localhost:9000 --aws_access_key_id minioadmin --aws_secret_access_key minioadmin
```