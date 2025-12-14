import warnings
from typing import Union, Optional

import pymongo
from bson import ObjectId

# warnings.warn("The AsyncMongoAPI code is untested and may have bugs.")


class AsyncMongoAPI:
    client: pymongo.AsyncMongoClient
    db_name: str

    def close(self):
        """
        Close the connection to MongoDB

        :return:
        """
        self.client.close()

    def __init__(self,
                 db_address: str,
                 db_name: str,
                 db_username: str,
                 db_password: str,
                 service: str = "mongodb",
                 **kwargs):
        """
        :param db_address: Database Address like db.something.mongodb.net
        :param db_name: name of the database
        :param db_username: username
        :param db_password: password to username

        :param kwargs: Passed to the MongoClient.__init__ method. (i.e. tlsCAFile=certifi.where())
        """
        if service not in ("mongodb+srv", "mongodb"):
            raise ValueError("service must be either 'mongodb+srv' or 'mongodb'")

        # initialising connection to Mongo
        self.client = pymongo.AsyncMongoClient(f"{service}://{db_username}:{db_password}@{db_address}/"
                                               f"{db_name}?retryWrites=true&w=majority", **kwargs)

        self.db_name = db_name

    # INFO: Might be async
    def collection(self, collection: str):
        """
        Get a collection from the given database.

        :param collection: Collection name to retrieve
        :return:
        """
        return self.client[self.db_name][collection]

    async def find_one(self,
                       collection: str,
                       filter_dict: dict = None,
                       projection_dict: dict = None,
                       sort: list = None) -> Optional[dict]:
        """
        Query the database.

        :param collection: Collection name string
        :param filter_dict: A dict specifying elements which must be present for a document to be included in the res
        :param projection_dict: A dict of field names that should be returned in the result
        :param sort: A list of (key, direction) pairs specifying the sort order for this query

        :return:
        """

        col = self.collection(collection)

        return await col.find_one(filter=filter_dict, projection=projection_dict, sort=sort)

    async def find(self,
                   collection: str,
                   filter_dict: dict = None,
                   projection_dict: dict = None,
                   sort: list = None,
                   skip:int = 0,
                   limit: int = 0) -> list[dict]:
        """
        Query the database.

        :param collection: Collection name string
        :param filter_dict: A dict specifying elements which must be present for a document to be included in the result
        :param projection_dict: A dict of field names that should be returned in the result
        :param sort: A list of (key, direction) pairs specifying the sort order for this query
        :param skip: Number of documents in order to skip
        :param limit: Number of documents to return

        :return:
        """

        col = self.collection(collection)

        a_cur = col.find(filter=filter_dict, projection=projection_dict, sort=sort, skip=skip, limit=limit)

        return [e async for e in a_cur]

    async def insert_one(self,
                         collection: str,
                         document_dict: dict = None) -> ObjectId:
        """
        Insert a single document.

        :param collection: Collection name string
        :param document_dict:  The document to insert

        :return: inserted id
        """
        if document_dict is None:
            document_dict = {}

        col = self.collection(collection)

        result = await col.insert_one(document=document_dict)

        return result.inserted_id

    async def insert(self,
                     collection: str,
                     document_list: list = None) -> list[ObjectId]:
        """
        Insert an iterable of documents.

        :param collection: Collection name string
        :param document_list:  The documents to insert into the db. Needs to be a list containing documents

        :return: inserted id
        """
        if document_list is None or len(document_list) < 1:
            return []

        col = self.collection(collection)

        result = await col.insert_many(documents=document_list)

        return result.inserted_ids

    async def update_one(self,
                         collection: str,
                         filter_dict: dict = None,
                         update_dict: dict | list = None,
                         upsert: bool = False) -> int:
        """
        Update a single document matching the filter.

        :param collection: Collection name string
        :param filter_dict: A dict specifying elements which must be present for a document to be included in the result
        :param update_dict: A dict with the modifications to apply
        :param upsert: If True, perform an insert if no documents match the filter

        :return: modified count
        """

        col = self.collection(collection)

        result = await col.update_one(filter=filter_dict, update=update_dict, upsert=upsert)

        return result.modified_count

    async def update(self,
                     collection: str,
                     filter_dict: dict = None,
                     update_dict: Union[list, dict] = None,
                     upsert: bool = False) -> int:
        """
        Update one or more documents that match the filter.

        :param collection: Collection name string
        :param filter_dict: A dict specifying elements which must be present for a document to be included in the result
        :param update_dict: A dict with the modifications to apply
        :param upsert: If True, perform an insert if no documents match the filter

        :return: modified count
        """

        col = self.collection(collection)

        result = await col.update_many(filter=filter_dict, update=update_dict, upsert=upsert)

        return result.modified_count

    async def delete_one(self,
                         collection: str,
                         filter_dict: dict = None) -> int:
        """
        Delete a single document matching the filter.

        :param collection: Collection name string
        :param filter_dict A dict specifying elements which must be present for a document to be included in the result

        :return: deleted count
        """

        col = self.collection(collection)

        result = await col.delete_one(filter=filter_dict)

        return result.deleted_count

    async def delete(self,
                     collection: str,
                     filter_dict: dict = None) -> int:
        """
        Delete one or more documents matching the filter.

        :param collection: Collection name string
        :param filter_dict: A dict specifying elements which must be present for a document to be included in the result

        :return: deleted count
        """

        col = self.collection(collection)

        result = await col.delete_many(filter=filter_dict)

        return result.deleted_count

    async def count(self,
                    collection: str,
                    filter_dict: dict = None) -> int:
        """
        Count the number of documents in this collection.

        :param collection: Collection name string
        :param filter_dict: A dict specifying elements which must be present for a document to be included in the result

        :return:
        """
        if filter_dict is None:
            filter_dict = {}

        col = self.collection(collection)
        return await col.count_documents(filter=filter_dict)

    async def aggregate(self,
                        collection: str,
                        pipeline: list = None):
        """
        Perform an aggregation using the aggregation framework on this collection.

        :param collection: Collection name string
        :param pipeline: A list of aggregation pipeline stages

        :return:
        """
        if pipeline is None:
            pipeline = []

        col = self.collection(collection)
        a_cur = await col.aggregate(pipeline=pipeline)
        return await a_cur.to_list()

    async def find_one_and_update(self,
                            collection: str,
                            update_dict: Union[list, dict],
                            filter_dict: Union[dict, list] = None,
                            projection_dict: dict = None,
                            sort: list = None,
                            return_document: pymongo.ReturnDocument = pymongo.ReturnDocument.AFTER) -> dict | None:
        """
        Find a document and update it in one atomic operation.

        :param collection: Collection name string
        :param update_dict: A dict with the modifications to apply
        :param filter_dict: A dict specifying elements which must be present for a document to be included in the result
        :param projection_dict: A dict of field names that should be returned in the result
        :param sort: A list of (key, direction) pairs specifying the sort order for this query
        :param return_document: state in which the document is to be returned.

        :return:
        """
        if filter_dict is None:
            filter_dict = {}

        col = self.client[self.db_name][collection]

        result = await col.find_one_and_update(filter=filter_dict,
                                         update=update_dict,
                                         projection=projection_dict,
                                         sort=sort,
                                         # INFO, that's correct pymongo.ReturnDocument is a wrapper for bool.
                                         return_document=return_document)

        return result