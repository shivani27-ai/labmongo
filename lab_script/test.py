from pymongo import MongoClient
from pymongo.errors import OperationFailure
from copy import deepcopy

# Configure the MongoDB connection
client = MongoClient('mongodb://localhost:27017')
db = client['labautonomy']
collection = db['cp_lab']
changes_dict = {}
# Set the pipeline for Change Streams
pipeline = [{'$match': {'operationType': {'$in': ['insert', 'update', 'delete']}}}]

# Open the Change Stream
try:
    with collection.watch(pipeline) as stream:
        for change in stream:
            print('llll')
            document_id = change['documentKey']['_id']
            operation_type = change['operationType']

            # Fetch the current document
            document = collection.find_one({'_id': document_id})

            if operation_type == 'insert':
                # If it's an insert operation, print the entire document
                print(document)
            elif operation_type == 'update':
                # If it's an update operation, apply the changes to a deep copy of the document
                update_description = change['updateDescription']
                updated_fields = update_description['updatedFields']
                document_copy = deepcopy(document)

                for field, value in updated_fields.items():
                    document_copy[field] = value

                # Print the entire document with the changes
                print(document_copy)
            elif operation_type == 'delete':
                # If it's a delete operation, print a message indicating the deletion
                print(f"Document with _id {document_id} has been deleted.")
except OperationFailure as e:
    print('Error:', e)
