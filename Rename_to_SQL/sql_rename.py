"""This module had script for Renaming the generated query file to sql format, change location"""
import os
import glob
import boto3

athena_client = boto3.client("athena")
s3_res = boto3.resource("s3")
File_Path = "s3://sqlcheck/databasename/tablename_"


def store_query_s3():
    """This method saves the sql query from Athena in S3 bucket in the given path"""
    query = "SHOW CREATE TABLE {}.{}".format("databasename", "tablename")
    response = athena_client.start_query_execution(
        QueryString=query, ResultConfiguration={"OutputLocation": File_Path}
    )
    print(response)
    main(File_Path)


class SqlRename:
    """This is the Rename Sql class in the module"""

    def __init__(self, bucket_name, database_name, table_name):
        """This is the init method of class SqlRename"""
        self.bucket_name = bucket_name
        self.database_name = database_name
        self.table_name = table_name

    def change_file_sql(self):
        """This method changes the file format to sql by getting file from s3"""
        my_bucket = s3_res.Bucket(self.bucket_name)
        for object in my_bucket.objects.all():
            full_path = object.key
            # file_name = os.path.basename(full_path)
            os.path.join(os.getcwd(), os.path.basename(full_path))
            for file in glob.glob("*.txt"):
                new_file = os.rename(file, self.table_name + ".sql")
            copy_source = {"Bucket": self.bucket_name, "Key": object.key}
            s3_res.Object(self.bucket_name, self.database_name / new_file).copy_from(
                CopySource=copy_source
            )
            s3_res.Object(self.bucket_name, object.key).delete()


def main(file_path):
    """This method get the bucket details from the path"""
    # print(file_path)
    split = file_path.split("/", 4)
    bucket_name = split[2]
    database_name = split[3]
    table_name = split[4]
    # print(database_name,table_name)
    check = SqlRename(bucket_name, database_name, table_name)
    check.change_file_sql()


if __name__ == "__main__":
    store_query_s3()
