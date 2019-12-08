from google.cloud import bigquery
from credConfig import credentialsPath
import os
import arrow
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credentialsPath

class BaseTool:
    def __init__(self):
        self.client = bigquery.Client()
        self.dataset_id = ""

    # Executes when instance defined at start of 'with' block
    def __enter__(self):
        return self

    # Executes when instance defined in 'with' block is removed from memory
    def __exit__(self, type, value, tb):
        for dataset_id in [d.dataset_id for d in self.client.list_datasets(self.client.project)]:
            self.client.delete_dataset(dataset_id, delete_contents=True, not_found_ok=True)  # Make an API request.
            print("Deleted dataset '{}'".format(dataset_id))

    # creates a new dataset with an option for custom naming
    def createDataset(self,customName=str(arrow.utcnow().timestamp),country="US"):
        dataset_id = "{}.{}".format(self.client.project,customName.replace(" ",""))
        dataset_already_exists = lambda x: True if x in [d.dataset_id for d in self.client.list_datasets(self.client.project)] else False
        if dataset_already_exists(customName):
            print("Dataset {} already exists".format(dataset_id))
        else:
            dataset = bigquery.Dataset(dataset_id)
            dataset.location = country
            dataset = self.client.create_dataset(dataset)
            print("Created dataset {}".format(dataset.dataset_id))
            return customName

    # attempts to change the current mounted dataset
    def setDataSet(self,customName):
        #candidate_dataset_id = "{}.{}".format(self.client.project,customName.replace(" ",""))
        dataset_exists = lambda x: True if x in [d.dataset_id for d in self.client.list_datasets(self.client.project)] else False
        if dataset_exists(customName):
            self.dataset_id = customName
            self.dataset = self.client.get_dataset(self.dataset_id)
        else:
            print("Dataset {} does not exist yet".format(customName))

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
            labels = self.dataset.labels
            if labels:
                for label, value in labels.items():
                    print("\t{}: {}".format(label, value))
            else:
                print("\tDataset has no labels defined.")
        else:
            print("No dataset loaded")

    def createDefaultTables(self):
        tables = [ 
                    ["Terminals",[["ID","INT64","REQUIRED"],["Name","STRING","REQUIRED"]]],

                    ["EntryTimes",[["ID","INT64","REQUIRED"],["Date","STRING","REQUIRED"]]],
                    
                    ["EntryInfo",[["EntryTimeID","INT64","REQUIRED"],["TerminalID","INT64","REQUIRED"],["Capacity","FLOAT64","REQUIRED"]]]
                ]
        
        for tableInfo in tables:
            self.createTable(tableInfo)

    def createTable(self,tableInfo):
        tableName = tableInfo[0]
        table_exists = lambda x: True if x in [d.table_id for d in self.client.list_tables(self.dataset)] else False
        if table_exists(tableName):
            print("Table {} Already exists".format(tableName))
        else:
            table_id = "{}.{}.{}".format(self.client.project,self.dataset_id,tableName)
            schema = []
            for column in tableInfo[1]:
                schema.append(bigquery.SchemaField(*column))

            table = bigquery.Table(table_id, schema=schema)

            table = self.client.create_table(table)

            print(
                    "Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id)
                )

    def outputTables(self,tables):
        print("Tables:")  # Make an API request(s).
        if tables:
            for table in tables:
                print("\t{}".format(table.table_id))
        else:
            print("\tThis dataset does not contain any tables.")


def fullOperation():
    with BaseTool() as bqClient:
        bqClient.dataset_id = bqClient.createDataset()
        bqClient.setDataSet(bqClient.dataset_id)
        bqClient.createDefaultTables()
    pass

if __name__ == "__main__":
    fullOperation()
    pass