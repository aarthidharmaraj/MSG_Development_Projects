"""This module had script for create crawler in s3 path of csv file,"""
import argparse
import csv
import json

import boto3

glue_client = boto3.client("glue", region_name="ap-south-1")


def create_crawler_aws():
    """This method creates the crwaler in AWS for details in CSV file"""
    exceptioncrawler = []

    with open("sample_crawler_definition.json", "r", encoding="UTF-8") as file:
        data = json.load(file)
        try:
            dict_filter = lambda x, y: dict([ (i,x[i]) for i in x if i in set(y) ])
            crawler_details=("Name","Role","DatabaseName","Description","Targets",
                            "Classifiers","TablePrefix","Schedule","SchemaChangePolicy",
                            "RecrawlPolicy","LineageConfiguration","LakeFormationConfiguration",
                            "Configuration","CrawlerSecurityConfiguration","Tags")
            matched=dict_filter(data,crawler_details)
            glue_client.create_crawler(**matched
            )
            print("Successfully created the crawler in aws")
        except Exception as excep:
            exceptioncrawler.extend([data["Name"], excep])
            print("\n Cannot be able to create the crawler in aws\n", exceptioncrawler)

class CreateCrawler:
    """This is the Crawler class in the module"""

    def __init__(self, crawler_name, database_name, s3_path, role):
        """This is the init method of class ModifyFile"""
        self.crawler_name = crawler_name
        self.database_name = database_name
        self.s3_path = s3_path
        self.role = role

    def check_crawler_name(self):
        """This method checks if the crawler name need to update in s3 path of csv file"""
        with open("crawler_details.csv", "r", encoding="utf8") as check_file:
            csvreader = csv.DictReader(check_file)
            check = []
            for row in csvreader:
                check.append(row["crawler_name"])
            if self.crawler_name in check:
                print("The crawler is already present")
                self.update()
            else:
                self.create_crawler_csv()

    def create_crawler_csv(self):
        """This method that creates a new crawler in s3 path of csv file"""
        datas_new = [self.crawler_name, self.database_name, self.s3_path, self.role]
        with open("crawler_details.csv", "a", encoding="utf-8") as append_file:
            write_data = csv.writer(append_file)
            write_data.writerow(datas_new)
            print("The new crawler details are added to the csv file")
            create_crawler_aws()

    def update(self):
        """if crawler_name exixts Update details of crawler in csv file and in json definition"""
        with open("crawler_details.csv", "r", encoding="utf-8") as file:
            csv_reader = csv.reader(file)
            rows=[]
            for rec in csv_reader:
                rows.append(rec)
                # print(rows)
            for i in range(1,len(rows)):
                # print(rows[i][0])
                if rows[i][0]==self.crawler_name:
                    rows[i][1] = self.database_name
                    rows[i][2] = self.s3_path
                    rows[i][3] = self.role
            with open("crawler_details.csv", "w", encoding="utf-8") as new_file:
                csv_writer = csv.writer(new_file,lineterminator='\n')
                csv_writer.writerows(rows)
        with open("sample_crawler_definition.json", "r", encoding="utf-8") as file:
            data = json.load(file)
        with open("crawler_details.csv", "r", encoding="utf-8") as csvfile:
            csv_reader = csv.DictReader(csvfile)
            for row in csv_reader:
                if row["crawler_name"] == data["Name"]:
                    strrole = str(row["role"])
                    strdatabase = str(row["database_name"])
                    strpath = str(row["s3_path"])
                    data["DatabaseName"] = strdatabase
                    data["Role"] = strrole
                    # data['Targets']['S3Targets']['path']=strpath
                with open(
                    "sample_crawler_definition.json", "w", encoding="utf-8"
                ) as jsonfile:
                    json.dump(data, jsonfile)
        create_crawler_aws()


def main():
    """This is the main method"""
    parser = argparse.ArgumentParser(
        description="This argparser used for getiing input"
    )
    parser.add_argument(
        "--crawler_name", type=str, help="Enter the crawler name you need to check"
    )
    parser.add_argument("--database_name", type=str, help="Enter the database name")
    parser.add_argument(
        "--s3_path", type=str, help="Enter the s3_path you need to check"
    )
    parser.add_argument("--role", type=str, help="Enter the role you need to check")
    args = parser.parse_args()
    if args.crawler_name:
        if args.s3_path:
            crawl = CreateCrawler(
                args.crawler_name, args.database_name, args.s3_path, args.role
            )
            crawl.check_crawler_name()
        else:
            print("The s3_path of the crawler must be present")
    else:
        print("The crawler name and s3_path must be present")


if __name__ == "__main__":
    main()
