from fastapi import FastAPI
from opensearchpy import OpenSearch

app = FastAPI(title="DVF API")
client = OpenSearch([{"host": "opensearch", "port": 9200}], use_ssl=False)
INDEX = "dvf-mutations"

@app.get("/stats/departement/{dept}")
def stats_dept(dept: str):
    res = client.search(index=INDEX, body={
        "size": 0,
        "query": {"term": {"code_departement": dept}},
        "aggs": {
            "nb_transactions": {"value_count": {"field": "valeur_fonciere"}},
            "prix_m2_moyen": {"avg": {"field": "prix_m2"}},
            "valeur_moyenne": {"avg": {"field": "valeur_fonciere"}}
        }
    })
    a = res["aggregations"]
    return {
        "departement": dept,
        "nb_transactions": a["nb_transactions"]["value"],
        "prix_m2_moyen": round(a["prix_m2_moyen"]["value"] or 0, 2),
        "valeur_moyenne": round(a["valeur_moyenne"]["value"] or 0, 2)
    }

@app.get("/top-communes/prix-m2")
def top_communes(size: int = 10):
    res = client.search(index=INDEX, body={
        "size": 0,
        "aggs": {
            "top_communes": {
                "terms": {"field": "code_commune", "size": size,
                          "order": {"prix_m2_moyen": "desc"}},
                "aggs": {
                    "prix_m2_moyen": {"avg": {"field": "prix_m2"}},
                    "nom": {"terms": {"field": "nom_commune.keyword", "size": 1}}
                }
            }
        }
    })
    results = []
    for b in res["aggregations"]["top_communes"]["buckets"]:
        nom = b["nom"]["buckets"][0]["key"] if b["nom"]["buckets"] else b["key"]
        results.append({"commune": nom, "code": b["key"],
                        "prix_m2_moyen": round(b["prix_m2_moyen"]["value"] or 0, 2)})
    return results

@app.get("/anomalies")
def anomalies(size: int = 100):
    res = client.search(index=INDEX, body={
        "size": size,
        "query": {"term": {"qualite_donnee": "SUSPECTE"}},
        "_source": ["nom_commune", "valeur_fonciere", "surface_bati", "prix_m2", "type_local"]
    })
    return [h["_source"] for h in res["hits"]["hits"]]

@app.get("/evolution/prix-m2/{dept}")
def evolution(dept: str):
    res = client.search(index=INDEX, body={
        "size": 0,
        "query": {"term": {"code_departement": dept}},
        "aggs": {
            "par_annee": {
                "terms": {"field": "annee", "order": {"_key": "asc"}},
                "aggs": {"prix_m2_moyen": {"avg": {"field": "prix_m2"}}}
            }
        }
    })
    return [
        {"annee": b["key"], "prix_m2_moyen": round(b["prix_m2_moyen"]["value"] or 0, 2)}
        for b in res["aggregations"]["par_annee"]["buckets"]
    ]
