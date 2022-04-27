"""This module had script for create crawler in s3 path of csv file,"""
import argparse
import csv
import boto3

glue_client = boto3.client("glue", region_name="ap-south-1")


def create_crawler_aws():
    """This method creates the crwaler in AWS for details in CSV file"""
    invalidcrawler = []
    existingcrawler = []
    with open("crawler_details_updated.csv", "r", encoding="utf-8") as file:
        csv_reader = csv.DictReader(file)
        for crawlers in csv_reader:
            try:
                glue_client.create_crawler(
                    Name=crawlers["crawler_name"],
                    Role=crawlers["role"],
                    DatabaseName=crawlers["database_name"],
                    Targets={"S3Targets": [{"Path": crawlers["s3_path"]}]},
                )
                print("Successfully created the crawler")
            except glue_client.exceptions.AlreadyExistsException:
                existingcrawler.append(crawlers["crawler_name"])
                # print("The provided crawler name is already present")
            except glue_client.exceptions.InvalidInputException:
                invalidcrawler.append(crawlers["crawler_name"])
                # print("The provided details are not valid")
            except glue_client.exceptions.OperationTimeoutException:
                print("The operation has been timed out")
            except glue_client.exceptions.ResourceNumberLimitExceededException:
                print("A resource numerical limit was exceeded")
            # except:
            #     print("Cannot create a crawler")
    print("The Invalid Input crawlers are", invalidcrawler)
    print("The Already Exixted crawlers are", existingcrawler)
    print("\n Cannot be able to create the crawler because of the above errors")


class CreateCrawler:
    """This is the Crawler class in the module"""

    def __init__(self, crawler_name, database_name, role, s3_path):
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
        with open("crawler_details_updated.csv", "a", encoding="utf-8") as append_file:
            write_data = csv.writer(append_file)
            write_data.writerow(datas_new)
            print("The new crawler details are added to the csv file")
            create_crawler_aws()

    def update(self):
        """if crawler_name is present,Update the details of crawler in a new csv file"""
        with open("crawler_details.csv", "r", encoding="utf-8") as file:
            csv_reader = csv.DictReader(file)
            with open("crawler_details_updated.csv", "a", encoding="utf-8") as new_file:
                csv_writer = csv.DictWriter(new_file, fieldnames=csv_reader.fieldnames)
                # csv_writer.writeheader()
                for row in csv_reader:
                    if row["crawler_name"] == self.crawler_name:
                        row["database_name"] = self.database_name
                        row["s3_path"] = self.s3_path
                        row["role"] = self.role
                        csv_writer.writerow(row)


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
