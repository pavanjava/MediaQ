# MediaQ
a platform powered by litserve and qdrant for blazing fast information retrieval and serving

### How to run
- run `pip install -r requirements.txt`
- change the respective values of `.env` file
- run `the az_blob_connector.py`
- place some file in blob and see if that get pulled to local data directory and indexed in qdrant.
- run `information_retrieval.py` to ask questions and get responses back.
