import os
import logging
import certifi
from pymongo import MongoClient, ASCENDING, errors
from src import drop_collections

log = logging.getLogger()
log.setLevel(os.environ.get('LOGGING_LEVEL', logging.INFO))


def load_environment_variables():
    global MONGO_URI
    MONGO_URI = os.environ.get('MONGO_URI')
    if not MONGO_URI:
        raise RuntimeError('MONGO_URI variable not configured in environment!')

    if 'DB_NAME' not in os.environ or not os.environ['DB_NAME']:
        raise RuntimeError('DB_NAME variable not configured in environment!')
    global DB_NAME
    DB_NAME = os.environ['DB_NAME']

    if 'ENVIRONMENT' not in os.environ or not os.environ['ENVIRONMENT']:
        raise RuntimeError('ENVIRONMENT variable not configured in environment!')
    global ENVIRONMENT
    ENVIRONMENT = os.environ['ENVIRONMENT']


load_environment_variables()

# Conexão ao MongoDB
client = MongoClient(MONGO_URI, tlsCAFile=certifi.where())
db = client[DB_NAME]

collection_types = [
    '.opendata.reports',
    '.private.reports',
    '.hybridflow.client.reports',
    '.hybridflow.server.reports',
    '.private.reports.structurepending'
]

# Configuração dos índices
default_index_config = [
    {
        "fields": {"idempotencyId": ASCENDING},
        "name": "idempotency_idx",
        "unique": True,
        "sparse": False
    }
]

private_and_open_data_index_config = [
    {
        "fields": {"idempotencyId": ASCENDING},
        "name": "idempotency_idx",
        "unique": True,
        "sparse": False
    },
    {
        "fields": {"status": ASCENDING, "timestamp": ASCENDING},
        "name": "status_timestamp_idx",
        "unique": False,
        "sparse": False
    }
]

drop_reason_consent_index_config = [
    {
        "fields": {"consentId": ASCENDING},
        "name": "idx1",
        "unique": True,
        "sparse": False
    }
]

drop_reason_fapi_index_config = [
    {
        "fields": {"fapiInteractionId": ASCENDING},
        "name": "fapi_idx",
        "unique": True,
        "sparse": False
    }
]

hybridflow_index_config = [
    {
        "fields": {"idempotencyId": ASCENDING},
        "name": "idempotency_idx",
        "unique": True,
        "sparse": False
    },
    {
        "fields": {"timestamp": ASCENDING, "status": ASCENDING, "step": ASCENDING},
        "name": "ts_st_sp_idx",
        "unique": False,
        "sparse": False
    }
]


# Função para gerar nomes de coleções
def build_collection_names(customer_ids, collection_types):
    collections = []
    for customer in customer_ids:
        for collection_type in collection_types:
            collection_type = collection_type if collection_type.startswith(".") else f".{collection_type}"
            collection = f"{ENVIRONMENT}.{customer}{collection_type}"
            log.debug(f"Collection built {collection}")
            collections.append(collection)
    return collections


def extract_customer_id(collection_name):
    parts = collection_name.split('.')
    if len(parts) > 2:
        log.debug(f"CustomerId extracted: {parts[1]}")
        return parts[1]  # O customerId é o segundo elemento

    return None


# Função para criar ou refazer índices
def create_or_update_indexes(collection, index_configs):
    for config in index_configs:
        fields = config["fields"]
        idx_name = config["name"]

        existing_indexes = collection.index_information()

        if idx_name not in existing_indexes:
            log.debug(f"Creating index {idx_name} on collection {collection.name}")
            try:
                collection.create_index(list(fields.items()), name=idx_name, unique=config["unique"], sparse=config["sparse"])
            except Exception as e:
                log.debug(f"Index creation failed for {idx_name} on collection {collection.name}: {e}")
        else:
            log.debug(f"Index {idx_name} already exists with correct order on collection {collection.name}")


def copy_documents_with_status(old_collection, new_collection, statuses, batch_size=100):
    query = {"status": {"$in": statuses}}
    cursor = old_collection.find(query)

    copied_count = 0
    batch = []
    for doc in cursor:
        batch.append(doc)
        if len(batch) == batch_size:
            new_collection.insert_many(batch)
            copied_count += len(batch)
            batch = []
            log.debug(f"Copied {copied_count} documents so far...")

    if batch:
        new_collection.insert_many(batch)
        copied_count += len(batch)
        log.debug(f"Copied a total of {copied_count} documents with status {statuses} from {old_collection.name} to {new_collection.name}")


def lambda_handler(event, context):
    try:
        collections = db.list_collection_names()

        customer_ids = set()
        for collection in collections:
            if collection.startswith("todrop."):
                continue
            customer_id = extract_customer_id(collection)
            if customer_id:
                customer_ids.add(customer_id)

        # Gerar nomes de coleções
        collection_names = build_collection_names(customer_ids, collection_types)

        # Process each collection to create or update indexes
        for collection_name in collection_names:
            new_collection_name = f"todrop.{collection_name}"
            try:
                # Rename collection
                new_collection = rename_collection(collection_name, new_collection_name)
                # Process collection indexes
                process_collection_indexes(collection_name, new_collection)

                copy_documents_with_status(db[new_collection_name], db[collection_name],
                                           ["PROCESSING", "DELIVERING"])
            except errors.OperationFailure as e:
                print(f"Failed to rename or create indexes for collection {collection_name}: {e}")
            except Exception as e:
                print(f"Unexpected error processing collection {collection_name}: {e}")

        log.debug("Renaming completed successfully.")
        log.debug("Dropping old collections...")
        drop_collections.drop_collections(MONGO_URI, DB_NAME)
        log.debug("Drop completed successfully.")
    except errors.ConnectionFailure as e:
        log.error(f"Failed to connect to MongoDB: {e}")
    except Exception as e:
        log.error(f"Unexpected error: {e}")
        raise


def process_collection_indexes(collection_name, new_collection):
    index_information = new_collection.index_information()
    print(f"Index information for {collection_name}: {index_information}")
    if collection_name.endswith('.private.reports.structurepending'):
        create_or_update_indexes(new_collection, drop_reason_consent_index_config)
        create_or_update_indexes(new_collection, drop_reason_fapi_index_config)
    elif collection_name.endswith('.private.reports') or collection_name.endswith('.opendata.reports'):
        create_or_update_indexes(new_collection, private_and_open_data_index_config)
    elif collection_name.endswith('.hybridflow.client.reports') or collection_name.endswith(
            '.hybridflow.server.reports'):
        create_or_update_indexes(new_collection, hybridflow_index_config)
    else:
        create_or_update_indexes(new_collection, default_index_config)

    print(f"Indexes processed for {collection_name}")


def rename_collection(collection_name, new_collection_name):
    db[collection_name].rename(new_collection_name)
    # Criar uma nova coleção com o nome original
    db.create_collection(collection_name)
    new_collection = db[collection_name]
    return new_collection


# Para testes locais
if __name__ == "__main__":
    lambda_handler(None, None)
