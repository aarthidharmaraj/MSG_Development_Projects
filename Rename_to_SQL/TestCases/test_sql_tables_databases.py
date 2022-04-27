'''This module has script for testing mocking of AWS services, tests the python script of changing file in sql format'''
import os
import pytest
import boto3
from moto import mock_athena, mock_glue, mock_s3


@pytest.fixture
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"


@mock_athena
def test_start_query_execution():
    """This method tests the start query execution of athena"""
    client = boto3.client("athena", region_name="us-east-1")

    sec_response = client.start_query_execution(
        QueryString="query2",
        QueryExecutionContext={"Database": "string"},
        ResultConfiguration={"OutputLocation": "string"},
    )
    assert "QueryExecutionId" in sec_response


@mock_glue
def test_create_database():
    """This method tests the create database in glue from mocked AWS services"""
    glue_client = boto3.client("glue", region_name="us-east-1")
    glue_client.create_database(
        DatabaseInput={
            "Name": "SampleTestDatabase",
            "Description": "Sample database created for testing",
        }
    )


@mock_glue
def test_get_databases():
    """This method tests for getting databases"""
    glue2_client = boto3.client("glue", region_name="us-east-1")
    glue2_client.get_databases()
    # dbname = response["DatabaseList"]


def function_under_test():
    """This method get the objects from bucket and pass to the test function"""
    s3_client = boto3.client("s3")
    obj = s3_client.get_object(Bucket="test-bucket", Key="test-key.txt")
    return obj["Body"].read().decode("utf-8")


@pytest.fixture
def s3_mock():
    """This method creates a mock for s3 bucket"""
    with mock_s3():
        conn = boto3.resource("s3", region_name="us-east-1")
        bucket = conn.create_bucket(Bucket="test-bucket")
        bucket.put_object(Body="sample test for bucket", Key="test-key.txt")
        yield


def test_function(s3_mock):
    """This method tests the get object from bucket"""
    assert function_under_test() == "sample test for bucket"


client = boto3.client("s3")
s3 = boto3.resource("s3")


@mock_s3
def test_mock_client_or_resource():
    """Testing with patch client and patch resource"""
    from moto.core import patch_client, patch_resource

    patch_client(client)
    patch_resource(s3)

    assert client.list_buckets()["Buckets"] == []

    assert list(s3.buckets.all()) == []


from check import change_file_sqlt


def change_sql_undertest():
    """Checks for the python script build for changing sql name"""
    s3_client2 = boto3.resource("s3")
    database_name = "database1"
    table_name = "table1"
    bucket_name = "test-bucket"
    response = change_file_sqlt(s3_client2, bucket_name, database_name, table_name)
    # return obj["Body"].read().decode("utf-8")
    return response


@pytest.fixture
def s3_mock_func():
    """This method creates a mock for s3 bucket"""
    with mock_s3():
        conn = boto3.resource("s3", region_name="us-east-1")
        bucket = conn.create_bucket(Bucket="test-bucket")
        bucket.put_object(
            Body="sample test for bucket", Key="database1/table1/flow.txt"
        )
        yield


def test_function2(s3_mock_func):
    """This is a test method for calling function"""
    assert change_sql_undertest() == "success"
