# server.py
import litserve as ls
from qdrant_ops import QdrantOperations


# STEP 1: DEFINE YOUR MODEL API
class InformationRetrievalLitAPI(ls.LitAPI):

    def __init__(self):
        self.qdrant_operations = None

    def setup(self, device):
        # instantiation will create the collection
        self.qdrant_operations = QdrantOperations()

    def decode_request(self, request):
        return request["query"]

    def predict(self, query):
        # index the data into collection
        return self.qdrant_operations.query_with_rrf(query_text=query[0])

    def encode_response(self, output):
        return {"output": output, "device": self.device}


# STEP 2: START THE SERVER
if __name__ == "__main__":
    api = InformationRetrievalLitAPI()
    server = ls.LitServer(api, accelerator="mps", max_batch_size=10, batch_timeout=1.0)
    server.run(port=8000)
