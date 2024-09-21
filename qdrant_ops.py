import os
import json
import tqdm
from typing import Text
from fastembed import TextEmbedding, LateInteractionTextEmbedding
from qdrant_client import QdrantClient, models
from dotenv import load_dotenv, find_dotenv

_ = load_dotenv(find_dotenv())


class QdrantOperations:
    def __init__(self, collection_name: Text = os.environ.get("QDRANT_COLLECTION_NAME")):
        self.dense_embedding_model_1 = TextEmbedding(model_name=os.environ.get("DENSE_MODEL_1"))
        self.dense_embedding_model_2 = TextEmbedding(model_name=os.environ.get("DENSE_MODEL_2"))
        self.late_interaction_embedding_model = LateInteractionTextEmbedding(os.environ.get("LATE_INTERACTION_MODEL"))

        self.client = QdrantClient(host=os.environ['QDRANT_HOST'],
                                   port=int(os.environ['QDRANT_PORT']),
                                   api_key=os.environ['QDRANT_API_KEY'])

        self.collection_name = collection_name
        self.dense_embeddings = None
        self.sparse_embeddings = None
        self.late_interaction_embeddings = None
        self.full_text = None
        self.segment_text = None
        self.data = None

        self._create_collection()

    def _get_dimensions(self):
        with open(file='data/final.json', mode='r') as data:
            self.data = json.load(data)
            self.full_text = self.data[0].get('full_text')
            self.segment_text = self.data[0].get('text')

        self.dense_embeddings_1 = list(self.dense_embedding_model_1.passage_embed(self.full_text))
        self.dense_embeddings_2 = list(self.dense_embedding_model_2.passage_embed(self.segment_text))
        self.late_interaction_embeddings = list(
            self.late_interaction_embedding_model.passage_embed(self.segment_text))

    # Create a simple function that will yield batches of data from a list
    def batchify(self, data, batch_size):
        for i in range(0, len(data), batch_size):
            yield data[i:i + batch_size]

    def _create_collection(self):
        self._get_dimensions()

        if not self.client.collection_exists(collection_name=self.collection_name):
            self.client.create_collection(
                collection_name=self.collection_name,
                vectors_config={
                    "all-MiniLM-L6-v2": models.VectorParams(
                        size=len(self.dense_embeddings_1[0]),
                        distance=models.Distance.COSINE
                    ),
                    "colbertv2.0": models.VectorParams(
                        size=len(self.late_interaction_embeddings[0][0]),
                        distance=models.Distance.COSINE,
                        multivector_config=models.MultiVectorConfig(
                            comparator=models.MultiVectorComparator.MAX_SIM
                        )
                    ),
                    "paraphrase-multilingual-MiniLM-L12-v2": models.VectorParams(
                        size=len(self.dense_embeddings_2[0]),
                        distance=models.Distance.COSINE
                    ),
                }
            )

    def insert_data(self):
        batch_size = 4
        for i, batch in enumerate(
                tqdm.tqdm(self.batchify(data=self.data, batch_size=batch_size), total=len(self.data) // batch_size)):
            dense_embeddings_1 = list(self.dense_embedding_model_1.passage_embed([item['full_text'] for item in batch]))
            dense_embeddings_2 = list(self.dense_embedding_model_2.passage_embed([item['text'] for item in batch]))
            late_interaction_embeddings = list(
                self.late_interaction_embedding_model.passage_embed([item['text'] for item in batch]))

            self.client.upsert(
                collection_name=self.collection_name,
                points=[
                    models.PointStruct(
                        id=int(batch[i]["_id"]),
                        vector={
                            "all-MiniLM-L6-v2": dense_embeddings_1[i].tolist(),
                            "paraphrase-multilingual-MiniLM-L12-v2": dense_embeddings_2[i].tolist(),
                            "colbertv2.0": late_interaction_embeddings[i].tolist(),
                        },
                        payload={
                            "_id": batch[i]["_id"],
                            "start": batch[i]["start"],
                            "end": batch[i]["end"],
                            "text": batch[i]["text"],
                            "full_text": batch[i]["full_text"]
                        }
                    )
                    for i, _ in enumerate(batch)
                ]
            )

    def query_with_rrf(self, query_text: str = ''):
        dense_query_vector_2 = next(self.dense_embedding_model_2.embed(query_text)).tolist()
        later_interaction_vector = next(self.late_interaction_embedding_model.embed(query_text)).tolist()
        prefetch = [
            models.Prefetch(
                query=dense_query_vector_2,
                using="paraphrase-multilingual-MiniLM-L12-v2",
                limit=20,
            ),
            models.Prefetch(
                query=later_interaction_vector[0],
                using="colbertv2.0",
                limit=20,
            )
        ]

        results = self.client.query_points(
            collection_name=self.collection_name,
            prefetch=prefetch,
            query=models.FusionQuery(
                fusion=models.Fusion.RRF
            ),
            with_payload=True,
            limit=10,
        )
        return results

# if __name__ == '__main__':
#     qdrant_ops = QdrantOperations(collection_name='audio_video_index')
#     qdrant_ops.insert_data()
#     response = qdrant_ops.query_with_rrf(query_text="What are the Symptoms of IBD?")
#     for str, scored_points in response:
#         for scored_point in scored_points:
#             print(f"start: {scored_point.payload.get('start')}, end: {scored_point.payload.get('end')}, text: {scored_point.payload.get('text')}")
