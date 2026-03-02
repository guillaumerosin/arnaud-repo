from elasticsearch import Elasticsearch

es = Elasticsearch(
    "http://172.20.0.201:9200",   # ⚠️ 9200 = elasticsearch & 5601 = kibana
    basic_auth=("user_kawasaki", "wTwF0UQRqL4it4j"),
)

try:
    if es.ping():
        print("Connexion à Elasticsearch OK ✅")
    else:
        print("Connexion établie mais ping échoué ❌")
except Exception as e:
    print("Erreur de connexion ❌")
    print(e)

