curl -s -X PUT 'http://localhost:6333/collections/rag_docs' \
  -H 'Content-Type: application/json' \
  -d '{
        "vectors": { "size": 768, "distance": "Cosine" }
      }' | jq .
// test the collection
curl -s http://localhost:6333/collections | jq