from main import EmbeddingRequest, MODEL_ID, app, embed


class FakeEmbeddings:
    def tolist(self):
        return [[1.0, 0.0], [0.0, 1.0]]


class FakeModel:
    def encode(self, texts, normalize_embeddings):
        assert texts == ["hello", "world"]
        assert normalize_embeddings is True
        return FakeEmbeddings()


app.state.model = FakeModel()
response = embed(EmbeddingRequest(texts=["hello", "world"]))
assert response.model == MODEL_ID
assert response.embeddings == [[1.0, 0.0], [0.0, 1.0]]
