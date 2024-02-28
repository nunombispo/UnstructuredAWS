from multiprocessing import freeze_support

from unstructured.ingest.connector.elasticsearch import ElasticsearchWriteConfig
from unstructured.ingest.connector.fsspec.s3 import S3AccessConfig, SimpleS3Config
from unstructured.ingest.connector.local import SimpleLocalConfig
from unstructured.ingest.connector.opensearch import SimpleOpenSearchConfig, OpenSearchAccessConfig
from unstructured.ingest.interfaces import (
    PartitionConfig,
    ProcessorConfig,
    ReadConfig, ChunkingConfig, EmbeddingConfig
)
from unstructured.ingest.runner import S3Runner, LocalRunner
from decouple import config
from unstructured.ingest.runner.writers.base_writer import Writer
from unstructured.ingest.runner.writers.opensearch import (
    OpenSearchWriter,
)


# Read from S3 bucket
def read_from_s3(bucket_url):
    runner = S3Runner(
        processor_config=ProcessorConfig(
            verbose=True,
            output_dir="s3-small-batch-output",
            num_processes=2,
        ),
        read_config=ReadConfig(),
        partition_config=PartitionConfig(
            partition_by_api=True,
            api_key=config("UNSTRUCTURED_API_KEY"),
            encoding="utf-8",
        ),
        connector_config=SimpleS3Config(
            access_config=S3AccessConfig(
                anon=True,
            ),
            remote_url=bucket_url,
        ),
    )
    runner.run()


def get_writer() -> Writer:
    return OpenSearchWriter(
        connector_config=SimpleOpenSearchConfig(
            access_config=OpenSearchAccessConfig(
                hosts=config("OPENSEARCH_HOSTS"),
                username=config("OPENSEARCH_USERNAME"),
                password=config("OPENSEARCH_PASSWORD"),
                use_ssl=True
            ),
            index_name=config("OPENSEARCH_INDEX_NAME"),
        ),
        write_config=ElasticsearchWriteConfig(
            batch_size_bytes=15_000_000,
            num_processes=2,
        ),
    )


def write_to_opensearch(bucket_url):
    writer = get_writer()
    runner = S3Runner(
        processor_config=ProcessorConfig(
            verbose=True,
            output_dir="local-output-to-opensearch",
            num_processes=2,
        ),
        connector_config=SimpleS3Config(
            access_config=S3AccessConfig(
                anon=True,
            ),
            remote_url=bucket_url,
        ),
        read_config=ReadConfig(),
        partition_config=PartitionConfig(
            partition_by_api=True,
            api_key=config("UNSTRUCTURED_API_KEY"),
            encoding="utf-8",
        ),
        chunking_config=ChunkingConfig(chunk_elements=True),
        embedding_config=EmbeddingConfig(
            provider="langchain-huggingface",
        ),
        writer=writer,
        writer_kwargs={},
    )
    runner.run()


if __name__ == '__main__':
    freeze_support()
    # read_from_s3("s3://utic-dev-tech-fixtures/small-pdf-set/")
    write_to_opensearch(bucket_url="s3://utic-dev-tech-fixtures/small-pdf-set/")
