import uvicorn
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from dotenv import load_dotenv, find_dotenv
from az_blob_operations import BlobStorageOperations
from qdrant_ops import QdrantOperations

_ = load_dotenv(find_dotenv())

app = FastAPI()


def init_global_config():
    _blob_operations = BlobStorageOperations()
    return _blob_operations


blob_operations = init_global_config()


@app.post("/event")
async def blob_event_listener(request: Request):
    event_data = await request.json()
    for _event in event_data:
        if _event.get('eventType') == 'Microsoft.EventGrid.SubscriptionValidationEvent':
            validation_code = _event['data']['validationCode']
            validation_response = {
                "validationResponse": validation_code
            }
            return JSONResponse(content=validation_response)
        elif _event.get('eventType') == 'Microsoft.Storage.BlobCreated':
            print(f"New blob created: {_event['data']['url']}")
            # Add your custom logic here
            blob_url: str = _event['data']['url']
            blob_name = blob_url[blob_url.rfind("/") + 1:]
            print(f"blob_name={blob_name}")

            # download the plain document and do all the process manually
            blob_operations.download_blob(blob_name=blob_name)

            # instantiation will create the collection
            _qdrant_operations = QdrantOperations()
            # index the data into collection
            _qdrant_operations.insert_data()

        elif _event.get('eventType') == 'Microsoft.Storage.BlobDeleted':
            blob_url: str = _event['data']['url']
            blob_name = blob_url[blob_url.rfind("/") + 1:]
            print(f"blob = {blob_name} deleted")


if __name__ == "__main__":
    uvicorn.run(app, host='127.0.0.1', port=5000, log_level="debug")
