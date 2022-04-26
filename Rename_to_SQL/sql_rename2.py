"""This module had script for Renaming the generated query file to sql format, change location"""
import os
import boto3
from isort import file
athena_client = boto3.client("athena")
s3_res = boto3.resource("s3")
# File_Path = "s3://msg-practice-induction/athena"


class SqlRename:
    """This is the Rename Sql class in the module"""

    def __init__(self, bucket_name, database_name, table_name):
        """This is the init method of class SqlRename"""
        self.bucket_name = bucket_name
        self.database_name = database_name
        self.table_name = table_name

    def store_sql_query_s3(self):
        """This method saves the sql query from Athena in S3 bucket in the given path"""
        file_path = "s3://" + self.bucket_name +"athena" + self.database_name + self.table_name
        query = "SHOW CREATE TABLE {}.{}".format(self.database_name, self.table_name)
        response = athena_client.start_query_execution(
            QueryString=query, ResultConfiguration={"OutputLocation": file_path}
        )
        # print(response)
        self.change_file_sql()

    def change_file_sql(self):
        """This method changes the file format to sql by getting file from s3"""
        exceptionfile = []
        try:
            my_bucket = s3_res.Bucket(self.bucket_name)
            for obj in my_bucket.objects.filter(
                Prefix=self.database_name / self.table_name
            ):
                file_name = os.path.basename(obj.key)
                if obj.key.endswith(".txt"):
                    copy_source = {"Bucket": self.bucket_name, "Key": obj.key}
                    s3_res.Object(
                        self.bucket_name, self.database_name / self.table_name.sql
                    ).copy_from(CopySource=copy_source)
                    s3_res.Object(self.bucket_name, obj.key).delete()
                    # api_client = s3_res.meta.client
                    # response = api_client.copy_object(
                    #     Bucket=self.bucket_name,
                    #     Key=object.key,
                    #     ContentType=self.table_name.sql,
                    #     MetadataDirective="REPLACE",
                    #     CopySource=copy_source,
                    # )
                print("\nSuccessfully renamed and changed the location of sql file")
        except Exception as excep:
            exceptionfile.extend(file_name, excep)
            print(
                "\ncannot be renamed in [ ",
                self.table_name,
                " ] of [",
                self.database_name,
                " ]because of this error\n",
                exceptionfile,
            )


def main():
    """This is the main method for SQLRename class"""
    bucket_name = "msg-practice-induction"
    response_catalog = athena_client.list_data_catalogs()
    print(response_catalog)
    for name in response_catalog:
        cat_name = name["DataCatalogsSummary"].get("CatalogName")
        response_database = athena_client.list_databases(CatalogName=cat_name)
        for dbname in response_database:
            data_name = dbname["DatabaseList"].get("Name")
            response_table = athena_client.list_table_metadata(
                CatalogName=cat_name, DatabaseName=data_name
            )
            for table in response_table:
                table_name = table["TableMetadataList"].get("Name")
                # print(data_name,table_name)
                check = SqlRename(bucket_name, data_name, table_name)
                check.store_sql_query_s3()


if __name__ == "__main__":
    main()
