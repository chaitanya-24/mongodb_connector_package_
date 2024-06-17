from typing import Any
import os
import pandas as pd
from pymongo.mongo_client import MongoClient
import json
from ensure import ensure_annotations

class mongo_operation:
    __collection = None  # here I have created a private/protected variable
    __database = None

    def __init__(self, client_url: str, database_name: str, collection_name: str = None):
        self.client_url = client_url
        self.database_name = database_name
        self.collection_name = collection_name

    def create_mongo_client(self) -> MongoClient:
        client: MongoClient = MongoClient(self.client_url, ssl=True)
        return client

    def create_database(self) -> Any:
        if mongo_operation.__database is None:
            client = self.create_mongo_client()
            mongo_operation.__database = client[self.database_name]
        return mongo_operation.__database

    def create_collection(self, collection: str = None) -> Any:
        if collection is None:
            collection = self.collection_name
        if mongo_operation.__collection is None:
            database = self.create_database()
            mongo_operation.__collection = database[collection]
        return mongo_operation.__collection

    def insert_record(self, record: dict, collection_name: str) -> Any:
        if isinstance(record, list):
            for data in record:
                if not isinstance(data, dict):
                    raise TypeError("Record must be a dictionary")
            collection = self.create_collection(collection_name)
            collection.insert_many(record)
        elif isinstance(record, dict):
            collection = self.create_collection(collection_name)
            collection.insert_one(record)

    def bulk_insert(self, datafile: str, collection_name: str = None) -> None:
        self.path = datafile

        if self.path.endswith('.csv'):
            dataframe = pd.read_csv(self.path, encoding='utf-8')
        elif self.path.endswith(".xlsx"):
            dataframe = pd.read_excel(self.path, encoding='utf-8')
        else:
            raise ValueError("Unsupported file format. Only .csv and .xlsx are supported.")

        datajson = json.loads(dataframe.to_json(orient='records'))
        collection = self.create_collection(collection_name)
        collection.insert_many(datajson)

