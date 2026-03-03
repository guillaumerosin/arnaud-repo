# dag_kafka_to_elasticsearch.py
# Kafka topic "test" → Parse GDELT GKG → Transformations → Elasticsearch

import json
import logging
from datetime import datetime

import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator

from kafka import KafkaConsumer
from elasticsearch import Elasticsearch


# ─── CONFIG ───────────────────────────────────────────────────────────────────

KAFKA_BROKERS         = "172.20.0.51:9092"
KAFKA_TOPIC           = "test"
KAFKA_GROUP_ID        = "gdelt-elastic-group"
KAFKA_CONSUME_TIMEOUT = 60_000

ES_HOST     = "https://172.20.0.201:9200"
ES_USER     = "user_kawasaki" 
ES_PASSWORD = "wTwF0UQRqL4it4j"
ES_INDEX    = "gdelt-gkg"


# ─── COLONNES GDELT GKG 2.1 ───────────────────────────────────────────────────

GKG_COLUMNS = [
    "GKGRECORDID", "DATE", "SourceCollectionIdentifier", "SourceCommonName",
    "DocumentIdentifier", "V1COUNTS", "V2COUNTS", "V1THEMES", "V2THEMES",
    "V1LOCATIONS", "V2LOCATIONS", "V1PERSONS", "V2PERSONS",
    "V1ORGANIZATIONS", "V2ORGANIZATIONS", "V1.5TONE", "DATES", "GCAM",
    "SharingImage", "RelatedImages", "SocialImageEmbeds", "SocialVideoEmbeds",
    "Quotations", "AllNames", "Amounts", "TranslationInfo", "Extras",
]

def parse_gdelt_line(line: str) -> dict:
    parts = line.strip().split("\t")
    return {col: (parts[i].strip() if i < len(parts) else "") for i, col in enumerate(GKG_COLUMNS)}


# ─── TRANSFORMATEURS ──────────────────────────────────────────────────────────

SOURCE_TYPE_MAP = {
    1: "WEB",
    2: "CITATIONONLY",
    3: "CORE",
    4: "DTIC",
    5: "JSTOR",
    6: "NONTEXTUALSOURCE",
}

def transform_source_type(raw) -> str:
    try:
        code = int(raw)
        label, desc = SOURCE_TYPE_MAP.get(code, ("UNKNOWN", f"Code non référencé : {code}"))
        return f"[{code}] {label} — {desc}"
    except:
        return f"Valeur invalide : '{raw}'"


def transform_date(raw: str) -> str:
    """YYYYMMDDHHMMSS → YYYYMMDDHHMMSS normalisé (complète avec des 0 si partiel)."""
    val = str(raw).strip()
    if not val or val == "0":
        return "00000000000000"
    return val.ljust(14, "0")[:14]


def transform_v15tone(raw: str) -> str:
    if not raw: return "NA"
    parts = raw.strip().split(",")
    if len(parts) != 7:
        return f"Format invalide ({len(parts)} valeurs attendu 7)."
    try:
        tone, pos, neg, pol, act, self_, wc = (
            float(parts[0]), float(parts[1]), float(parts[2]),
            float(parts[3]), float(parts[4]), float(parts[5]),
            int(float(parts[6]))
        )
    except:
        return f"NA"

    if   tone < -10: tl = "extrêmement négatif"
    elif tone <  -5: tl = "très négatif"
    elif tone <  -2: tl = "négatif"
    elif tone < -0.5:tl = "légèrement négatif"
    elif tone <=  0.5:tl= "neutre"
    elif tone <=  2: tl = "légèrement positif"
    elif tone <=  5: tl = "positif"
    elif tone <= 10: tl = "très positif"
    else:            tl = "extrêmement positif"

    vocab = (f"vocabulaire négatif dominant ({neg}% vs {pos}%)" if neg > pos + 1 else
             f"vocabulaire positif dominant ({pos}% vs {neg}%)" if pos > neg + 1 else
             f"vocabulaire équilibré (positif {pos}%, négatif {neg}%)")
    al = "très actif"  if act   >= 8  else "modérément actif" if act   >= 3 else "passif"
    sl = "subjectif"   if self_ >= 2  else "légèrement personnel" if self_ >= 0.5 else "impersonnel"
    wl = "très court"  if wc < 100   else "court" if wc < 300 else "standard" if wc < 800 else "long" if wc < 2000 else "très long"

    return (f"Ton {tl} (score = {tone}). {vocab.capitalize()}. "
            f"Charge émotionnelle : {pol}. Style {al} (densité action = {act}). "
            f"Registre {sl} (self/group = {self_}). Document {wl} ({wc} mots).")


_CB = {}
try:
    import os, json as _json
    _cb_path = os.path.join(os.path.dirname(__file__), "gcam_codebook.json")
    with open(_cb_path, "r", encoding="utf-8") as f:
        _CB = _json.load(f)
except:
    pass

def _dlabel(d):
    if d < 0.5: return "anecdotique"
    if d < 1:   return "très faible"
    if d < 3:   return "faible"
    if d < 8:   return "modérée"
    if d < 15:  return "élevée"
    return "très élevée"

def transform_v2gcam(raw: str) -> str:
    if not raw: return "NA"
    entries = [e.strip() for e in raw.strip().split(",") if ":" in e]
    wc = 1
    for e in entries:
        k, _, v = e.partition(":")
        if k == "wc":
            try: wc = int(v)
            except: pass

    count_items, value_items = [], []
    for e in entries:
        k, _, v = e.partition(":")
        if k in ("wc", "nwc"): continue
        elif k.startswith("c"):
            dim = k[1:]
            try:
                count   = int(v)
                density = round((count / max(wc, 1)) * 100, 3)
                info    = _CB.get(dim)
                name    = f"{info[0]} / {info[1]}" if info else f"Dict.{dim.split('.')[0]} / dim.{dim}"
                count_items.append((density, f"{name} : {count} mots ({density}%, {_dlabel(density)})"))
            except: pass
        elif k.startswith("v"):
            dim = k[1:]
            try:
                score = float(v)
                info  = _CB.get(dim)
                name  = f"{info[0]} / {info[1]}" if info else f"Dict.{dim.split('.')[0]} / dim.{dim}"
                value_items.append(f"{name} : score = {round(score, 4)}")
            except: pass

    if not count_items and not value_items:
        return "NA"

    count_items.sort(key=lambda x: x[0], reverse=True)
    result = (f"Document de {wc} mots. {len(count_items)} dimensions, {len(value_items)} scores continus.\n"
              f"Top 10 dimensions par densité :\n" +
              "\n".join(f"  • {l}" for _, l in count_items[:10]))
    if value_items:
        result += "\nScores continus (extrait) :\n" + "\n".join(f"  • {l}" for l in value_items[:5])
    return result


def transform_v2dates(raw: str) -> str:
    if not raw: return "NA"
    blocks  = [b.strip() for b in raw.split(";#") if b.strip()]
    results = []
    for block in blocks:
        parts = [p for p in block.split("#") if p != ""]
        if len(parts) != 5: continue
        try:
            res, mo, d, y, offset = int(parts[0]), int(parts[1]), int(parts[2]), int(parts[3]), int(parts[4])
            if   res == 1: date_str = f"{y:04d}0000000000"
            elif res == 2: date_str = f"{y:04d}{mo:02d}00000000"
            elif res == 3: date_str = f"{y:04d}{mo:02d}{d:02d}000000"
            elif res == 4: date_str = f"0000{mo:02d}{d:02d}000000"
            else:          date_str = "00000000000000"
            pos = "titre/intro" if offset < 200 else "corps" if offset < 1000 else "conclusion"
            results.append(f"{date_str} (position {offset}, {pos})")
        except: continue
    if not results: return "NA"
    return f"{len(results)} date(s) mentionnée(s) :\n" + "\n".join(f"  • {r}" for r in results)


LOCATION_TYPE_MAP = {1:"Pays", 2:"Région / État / Province", 3:"Ville", 4:"Point d'intérêt", 5:"Entité mondiale"}

def transform_v2locations(raw: str) -> str:
    if not raw: return "NA"
    blocks  = [b.strip() for b in raw.split(";") if b.strip()]
    results = []
    for block in blocks:
        parts = block.split("#")
        if len(parts) < 3: continue
        try:
            lt  = LOCATION_TYPE_MAP.get(int(parts[0]), f"Type {parts[0]}")
            fn  = parts[1].strip()
            co  = parts[2].strip()
            lat = parts[5].strip() if len(parts) > 5 and parts[5] else None
            lon = parts[6].strip() if len(parts) > 6 and parts[6] else None
            s   = f"{lt} : {fn} ({co})"
            if lat and lon: s += f" — coordonnées : {lat}°N, {lon}°E"
            results.append(s)
        except: continue
    if not results: return "NA"
    return f"{len(results)} localisation(s) :\n" + "\n".join(f"  • {r}" for r in results)


def transform_list_field(raw: str, label: str) -> str:
    if not raw: return f"NA"
    items = [i.strip() for i in raw.split(";") if i.strip()]
    if not items: return f"NA"
    r = f"{len(items)} {label}(s) : {', '.join(items[:10])}"
    return r + (" [...]" if len(items) > 10 else ".")


def transform_message(raw: dict) -> dict:
    return {
        "id":               raw.get("GKGRECORDID", ""),
        "source":           raw.get("SourceCommonName", ""),
        "url":              raw.get("DocumentIdentifier", ""),
        "date_publication": transform_date(raw.get("DATE", "")),
        "source_type":      transform_source_type(raw.get("SourceCollectionIdentifier", "")),
        "tone":             transform_v15tone(raw.get("V1.5TONE", "")),
        "gcam":             transform_v2gcam(raw.get("GCAM", "")),
        "dates_in_text":    transform_v2dates(raw.get("DATES", "")),
        "persons":          transform_list_field(raw.get("V1PERSONS", ""),       "personne"),
        "organizations":    transform_list_field(raw.get("V1ORGANIZATIONS", ""), "organisation"),
        "themes":           transform_list_field(raw.get("V1THEMES", ""),        "thème"),
        "locations":        transform_v2locations(raw.get("V2LOCATIONS", "")),
        "image":            raw.get("SharingImage", "") or "NA",
        "ingested_at":      datetime.utcnow().isoformat() + "Z",
    }


# ─── FONCTION PRINCIPALE ──────────────────────────────────────────────────────

def index_to_elasticsearch(es, sensor_data: dict, doc_id: str):
    """Indexe un document transformé dans Elasticsearch (slide 49)."""
    try:
        es.index(
            index=ES_INDEX,
            id=doc_id,
            body=sensor_data
        )
    except Exception as e:
        logging.error(f"Failed to index data: {e}")


def consume_data(**context):
    # ── Connexion Elasticsearch (slide 49) ────────────────────────
    es = Elasticsearch(
        [ES_HOST],
        http_auth=(ES_USER, ES_PASSWORD),
        verify_certs=False,
        ssl_show_warn=False
    )

    try:
        logging.info(f"[ES] Connecté au cluster : {es.info()['cluster_name']}")
    except Exception as e:
        raise RuntimeError(f"[ES] Connexion impossible : {e}")

    # ── Connexion Kafka (slide 48) ─────────────────────────────────
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BROKERS,
        auto_offset_reset="earliest",          # earliest ou latest
        group_id=KAFKA_GROUP_ID,
        consumer_timeout_ms=KAFKA_CONSUME_TIMEOUT,
        value_deserializer=lambda x: x        # on garde les bytes bruts
    )

    count_ok = count_error = 0

    # ── Boucle de consommation (slide 48) ─────────────────────────
    for message in consumer:
        sensor_data = message.value            # bytes bruts

        try:
            raw_str = sensor_data.decode("utf-8", errors="replace").strip()

            # Détection JSON ou CSV GDELT
            if raw_str.startswith("{"):
                raw_dict = json.loads(raw_str)
            else:
                raw_dict = parse_gdelt_line(raw_str)

            # Transformation en document lisible humain
            doc = transform_message(raw_dict)

            # ID unique : clé Kafka → id GDELT → fallback partition-offset
            doc_id = (
                message.key.decode("utf-8") if message.key else None
                or doc.get("id")
                or f"{message.partition}-{message.offset}"
            )

            # Indexation dans Elasticsearch
            index_to_elasticsearch(es, doc, doc_id)
            logging.info(f"Indexed to Elasticsearch: {doc_id}")
            logging.info(doc)

            count_ok += 1

        except Exception as e:
            logging.error(f"Failed to index data: {e}")
            count_error += 1

    consumer.close()

    logging.info(
        f"\n[RÉSUMÉ] ──────────────────────────────────\n"
        f"  Topic    : {KAFKA_TOPIC}\n"
        f"  Indexés  : {count_ok}\n"
        f"  Erreurs  : {count_error}\n"
        f"  Index ES : {ES_INDEX}\n"
        f"────────────────────────────────────────────"
    )

    context["ti"].xcom_push(key="count_ok",    value=count_ok)
    context["ti"].xcom_push(key="count_error", value=count_error)


# ─── DAG ──────────────────────────────────────────────────────────────────────

with DAG(
    dag_id="kafka_to_elasticsearch",
    description="Kafka 'test' → Parse GDELT → Transformations → Elasticsearch",
    schedule_interval=None,
    start_date=datetime(2025, 2, 4),
    catchup=False,
    tags=["gdelt", "kafka", "elasticsearch", "phase4"]
) as dag:

    task = PythonOperator(
        task_id="consume_data",
        python_callable=consume_data,
        provide_context=True
    )
