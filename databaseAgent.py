from google.cloud import bigquery
import os
import arrow

class BaseTool:
    def __init__(self):
        self.client = bigquery.Client()
        self.dataset_id = ""
        self.dataset_id = self.createDataset()
        self.setDataSet(self.dataset_id)
        self.createDefaultTables()
        #self.insertDefaultTableRows(f"{self.client.project}.{self.dataset_id}.Terminals",getTerminals())

    # Executes when instance defined at start of 'with' block
    def __enter__(self):
        return self

    # Executes when instance defined in 'with' block is removed from memory
    def __exit__(self, type, value, tb):
        for dataset_id in [d.dataset_id for d in self.client.list_datasets(self.client.project)]:
            self.client.delete_dataset(dataset_id, delete_contents=True, not_found_ok=True)
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
        table_exists = lambda x: True if x in [d.table_id for d in self.client.list_tables(self.client.get_dataset(self.dataset_id))] else False
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
        print("Tables:")
        if tables:
            for table in tables:
                print("\t{}".format(table.table_id))
        else:
            print("\tThis dataset does not contain any tables.")

    # table_id: "your-project.your_dataset.your_table"
    # rows: 2d list where each element is a row
    def insertDefaultTableRows(self,table_id,rows):
        table = self.client.get_table(table_id)
        #query_job = self.client.query(f"""SELECT ID FROM `{self.client.project}.{self.dataset_id}.Terminals`""")

        print("Query results loaded to the table {}".format(table_id))
        errors = self.client.insert_rows(table, rows)
        if errors == []:
            print("New rows have been added.")
        else:
            print(errors)
        pass

    def insertTableRows(self,table_id,rows):
        table = self.client.get_table(table_id)
        
        if table_id.split(".")[2] == "EntryInfo":
            terminalTable = self.client.get_table(f"{self.client.project}.{self.dataset_id}.Terminals")
            entryTimeTable = self.client.get_table(f"{self.client.project}.{self.dataset_id}.EntryTimes")
            terminals = {x["Name"]:x["ID"] for x in self.client.list_rows(f"{self.client.project}.{self.dataset_id}.Terminals",selected_fields=terminalTable.schema)}
            timeentryid = [x["ID"] for x in self.client.list_rows(f"{self.client.project}.{self.dataset_id}.EntryTimes",selected_fields=entryTimeTable.schema)]
            timeentryid = len(timeentryid) + 1
            print(timeentryid,rows)
            self.client.insert_rows(entryTimeTable,[[timeentryid,rows[0][0]]])
            for row in rows:
                try:
                    row[1] = terminals[row[1]]                    
                except KeyError:
                    newTerminalID = 1
                    if len([x for x in terminals.keys()]) > 0:
                        newTerminalID = max([x for x in terminals.values()])+1
                    self.client.insert_rows(terminalTable,[[newTerminalID,row[1]]])
                    row[1] = newTerminalID
                    terminals = {x["Name"]:x["ID"] for x in self.client.list_rows(f"{self.client.project}.{self.dataset_id}.Terminals",selected_fields=terminalTable.schema)}
                row[0] = timeentryid

        errors = self.client.insert_rows(table, rows)
        if errors == []:
            print("New rows have been added.")

    def getTableData(self,table_id):
        table = self.client.get_table(f"{self.client.project}.{self.dataset_id}.{table_id}")
        if table_id == "EntryInfo":
            fullTableID = lambda x: f"{self.client.project}.{self.dataset_id}.{x}"
            getTable = lambda x: self.client.get_table(fullTableID(x))
            listRows = lambda x: self.client.list_rows(f"{self.client.project}.{self.dataset_id}.{x.table_id}", selected_fields=x.schema)
            terminalTable = getTable("Terminals")
            terminalQuery = {item["ID"]:item["Name"] for item in listRows(terminalTable)}
            entryTimeTable = getTable("EntryTimes")
            entryTimeQuery = {item["ID"]:item["Date"] for item in listRows(entryTimeTable)}
            latest = max([row for row in listRows(entryTimeTable)], key = lambda x: x["ID"])

            query = f"""
                SELECT *
                FROM `{self.client.project}.{self.dataset_id}.EntryInfo`
                WHERE EntryTimeID = {latest["ID"]}
            """
            query_job = self.client.query(query)
            rows = []
            fixtime = lambda x: arrow.get(int(entryTimeQuery[x])).to("US/Eastern").format("YYYY MM DD HH mm ss ZZ")
            for row in query_job.result():
                rows.append([fixtime(row[0]),terminalQuery[row[1]],row[2]])
                pass
            return rows

    def getAll(self,table_id="EntryInfo"):
        fullTableID = lambda x: f"{self.client.project}.{self.dataset_id}.{x}"
        getTable = lambda x: self.client.get_table(fullTableID(x))
        listRows = lambda x: self.client.list_rows(f"{self.client.project}.{self.dataset_id}.{x.table_id}", selected_fields=x.schema)
        terminalTable = getTable("Terminals")
        terminalQuery = {item["ID"]:item["Name"] for item in listRows(terminalTable)}
        entryTimeTable = getTable("EntryTimes")
        entryTimeQuery = {item["ID"]:item["Date"] for item in listRows(entryTimeTable)}
        latest = max([row for row in listRows(entryTimeTable)], key = lambda x: x["ID"])

        query = f"""
            SELECT *
            FROM `{self.client.project}.{self.dataset_id}.EntryInfo`
        """
        query_job = self.client.query(query)
        rows = []
        fixtime = lambda x: arrow.get(int(entryTimeQuery[x])).to("US/Eastern").format("YYYY MM DD HH mm ss ZZ")
        for row in query_job.result():
            rows.append([fixtime(row[0]),terminalQuery[row[1]],row[2]])
            pass
        return rows

def fullOperation():
    with BaseTool() as bqClient:
        bqClient.insertTableRows(f"{bqClient.client.project}.{bqClient.dataset_id}.EntryInfo",[["1575789100","C/D",0.5],["1575789100","B",0.2],["1575789100","A",0.9]])
        pass

if __name__=="__main__":
    fullOperation()