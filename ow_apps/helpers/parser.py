import argparse


VALID_BURST_BACKEND_OPTIONS = ["rabbitmq", "redis", "redis_stream", "redis_list", "s3"]
DEFAULT_DOCKER_IMAGE = "manriurv/rust-burst:1.72.1"


def add_terasort_to_parser(parser):
    parser.add_argument("--ts-endpoint", type=str, required=True,
                        help="Endpoint of the S3 service in which the terasort file is stored")
    parser.add_argument("--partitions", type=int, required=True, help="Number of partitions to sort the file into")
    parser.add_argument("--bucket", type=str, required=True, help="Terasort bucket name")
    parser.add_argument("--key", type=str, required=True, help="Terasort object key")


def add_burst_to_parser(parser):
    parser.add_argument("--granularity", type=int, required=False, help="Granularity of burst workers", default=None)
    parser.add_argument("--join", type=bool, required=False, help="Join burst workers in same invoker", default=False)
    parser.add_argument("--backend", type=str, required=True, help="Burst communication backend",
                        choices=VALID_BURST_BACKEND_OPTIONS)
    parser.add_argument("--chunk_size", type=int, required=False, help="Chunk size for burst messages (in KB)", default=1)


def add_openwhisk_to_parser(parser):
    parser.add_argument("--ow-host", type=str, required=True, help="Openwhisk host")
    parser.add_argument("--ow-port", type=int, required=True, help="Openwhisk port")
    parser.add_argument("--runtime-memory", type=int, required=False, help="Memory to allocate to the runtime (in MB)",
                        default=None)
    parser.add_argument("--custom-image", type=str, required=False, help="Tag of the docker custom image to use",
                        default=DEFAULT_DOCKER_IMAGE)


def try_or_except(parser):
    try:
        return parser.parse_args()
    except argparse.ArgumentError:
        parser.print_help()
        exit(1)
