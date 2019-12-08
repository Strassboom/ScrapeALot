from google.cloud import bigquery
from credConfig import credentialsPath
import os
import arrow
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credentialsPath

class BaseTool:
    def __init__(self):
        self.client = bigquery.Client()
        self.dataset_id = ""

    def __enter__(self):
        return self

    def __exit__(self, type, value, tb):
        for dataset_id in [d.dataset_id for d in self.client.list_datasets(self.client.project)]:
            self.client.delete_dataset(dataset_id, delete_contents=True, not_found_ok=True)  # Make an API request.
            print("Deleted dataset '{}'.".format(dataset_id))

    def __setDatasetId__(self, customName=arrow.now().format("YY-MM-DD-HH-mm-ss")):
        candidate_dataset_id = "{}.{}".format(self.client.project,customName.replace(" ",""))
        dataset_exists = lambda x: True if x in [d.dataset_id for d in self.client.list_datasets(self.client.project)] else False
        if dataset_exists(customName):
            self.dataset_id = customName
        else:
            print("Dataset with id {} does not exist yet".format(candidate_dataset_id))

    def getDatasetId(self):
        return self.dataset_id

    def createDataset(self,customName=arrow.now().format("YY-MM-DD-HH-mm-ss"),country="US"):
        dataset_id = "{}.{}".format(self.client.project,customName.replace(" ",""))
        dataset_already_exists = lambda x: True if x in [d.dataset_id for d in self.client.list_datasets(self.client.project)] else False
        if dataset_already_exists(customName):
            print("Dataset with id {} already exists".format(dataset_id))
        else:
            dataset = bigquery.Dataset(dataset_id)
            dataset.location = country
            dataset = self.client.create_dataset(dataset)
            print("Created dataset with id {}".format(dataset.dataset_id))

    def setDataSet(self,customName):
        # Set dataset_id to the ID of the dataset to fetch.
        candidate_dataset_id = "{}.{}".format(self.client.project,customName.replace(" ",""))
        dataset_exists = lambda x: True if x in [d.dataset_id for d in self.client.list_datasets(self.client.project)] else False
        if dataset_exists(customName):
            self.dataset_id = customName
            self.dataset = self.client.get_dataset(self.dataset_id)
        else:
            print("Dataset with id {} does not exist yet".format(candidate_dataset_id))

    def getDataSet(self):
        return self.dataset

    def outputDataSet(self):
        if self.dataset:
            full_dataset_id = "{}.{}".format(self.dataset.project, self.dataset.dataset_id)
            friendly_name = self.dataset.friendly_name
            print(
                "Got dataset '{}' with friendly_name '{}'.".format(
                    full_dataset_id, friendly_name
                )
            )

            # View dataset properties.
            print("Description: {}".format(self.dataset.description))
            print("Labels:")
            labels = dataset.labels
            if labels:
                for label, value in labels.items():
                    print("\t{}: {}".format(label, value))
            else:
                print("\tDataset has no labels defined.")
        else:
            print("No dataset loaded")

    def createDefaultTables(self,dataset):
        # Creates lists of queries that will create the tables we want
        createTerminals = self.client.query("""
                                        CREATE
                                            TABLE
                                                Terminals(
                                                    ID int UNIQUE,
                                                    Name text UNIQUE
                                                )""")

        createEntryTimes = self.client.query("""
                                        CREATE
                                            TABLE
                                                EntryTimes(
                                                    ID text UNIQUE,
                                                    Year int NOT NULL,
                                                    Month int NOT NULL,
                                                    Day int NOT NULL,
                                                    Hour int NOT NULL,
                                                    Minute int NOT NULL,
                                                    Second int NOT NULL
                                                )""")

        createEntries = self.client.query("""
                                    CREATE
                                        TABLE
                                            Entries(
                                                ID int UNIQUE,
                                                EntryTimeID text NOT NULL FOREIGN KEY REFERENCES EntryTimes.ID,
                                                TerminalID int NOT NULL FOREIGN KEY REFERENCES Terminals.ID,
                                                Capacity double NOT NULL
                                            )""")

        createTableQueries = [createTerminals,createEntryTimes,createEntries]

    def createTable(self,customName):
        table_id = "{}.{}.{}".format(self.client.project,self.dataset_id,customName)
        schema = [
        bigquery.SchemaField("ID", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("Name", "STRING", mode="REQUIRED"),
        ]

        table = bigquery.Table(table_id, schema=schema)
        table = self.client.create_table(table)  # Make an API request.
        print(
            "Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id)
        )

    def getTables(self,dataset):
       return list(self.client.list_tables(dataset))

    def getTable(self,dataset,table_id):
        self.client.get_table(table_id)

    def insertTableRows(self,table_id,rows):
        #table_id = "your-project.your_dataset.your_table"
        table = self.client.get_table(table_id)  # Make an API request.
        rows_to_insert = rows

        errors = self.client.insert_rows(table, rows_to_insert)  # Make an API request.
        if errors == []:
            print("New rows have been added.")

    def outputTables(self,tables):
        print("Tables:")  # Make an API request(s).
        if tables:
            for table in tables:
                print("\t{}".format(table.table_id))
        else:
            print("\tThis dataset does not contain any tables.")

def fullOperation():
    with BaseTool() as bqClient:
        bqClient.createDataset("MyFirstDataset")
        bqClient.setDataSet("MyFirstDataset")
        bqClient.createDefaultTables(bqClient.dataset)
    pass

if __name__ == "__main__":
    fullOperation()
    pass