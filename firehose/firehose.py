import json
import os
import uuid
from datetime import datetime, timedelta, timezone
import re
from atproto import (
    FirehoseSubscribeReposClient,
    parse_subscribe_repos_message,
    CAR,
    models,
)

import psycopg2
from kafka import KafkaProducer
import time

DBNAME = os.environ.get("DATABASE_NAME")
USER = os.environ.get("DATABASE_USER")
PASSWORD = os.environ.get("DATABASE_PWD")
HOST = os.environ.get("DATABASE_HOST")
DELAY = 15
client = None


def init_context(context):
    # read keywords
    conn = psycopg2.connect(dbname=DBNAME, user=USER, password=PASSWORD, host=HOST)
    keywords = get_keywords(conn)
    conn.close()

    # init producer
    producer = KafkaProducer(
        bootstrap_servers=[os.environ.get("KAFKA_BROKER")],
        key_serializer=lambda x: x.encode("utf-8"),
        value_serializer=lambda x: json.dumps(x, default=_iceberg_json_default).encode(
            "utf-8"
        ),
    )

    if not producer or not keywords:
        print("Producer or keywords not initialized. Can not start")
        return

    setattr(context, "producer", producer)
    setattr(context, "keywords", keywords)


def _iceberg_json_default(value):
    if isinstance(value, datetime):
        return value.isoformat()
    elif isinstance(value, set):
        return list(value)
    else:
        return repr(value)


def get_keywords(conn):
    """Get keywords from database"""
    cur = None
    data = []

    try:
        cur = conn.cursor()
        # use same keywords as news but localized
        query = "SELECT keyword_id, keyword, topic, language, country FROM news.search_keywords WHERE keyword_id < 1000 ORDER BY keyword_id"
        cur.execute(query)
        row = cur.fetchall()

        if row:
            for keyword_id, keyword, topic, language, country in row:
                config = {
                    "keyword_id": str(keyword_id),
                    "keyword": keyword,
                    "regex": re.compile(
                        r"\b" + re.escape(keyword.lower()).replace(r"\*", ".*") + r"\b",
                        re.IGNORECASE,
                    ),
                    "topic": topic,
                    "language": language,
                    "country": country,
                }
                data.append(config)
    except Exception as e:
        print("ERROR FIND KEYWORDS:")
        print(e)
    finally:
        cur.close()

    return data


def keyword_matches(text, keywords):
    """Filter text based on keywords"""
    match = []

    for k in keywords:
        if k["regex"].search(text):
            match.append([k["topic"], k["keyword"], k["language"]])

    return match


def serialize_extra(extra):
    """Serialize extra data for Kafka"""
    if isinstance(extra, dict):
        return json.dumps(extra, default=_iceberg_json_default)
    elif isinstance(extra, list):
        return json.dumps([json.dumps(e, default=_iceberg_json_default) for e in extra])
    elif isinstance(extra, bytes):
        return repr(extra)
    else:
        return str(extra)


def on_message_handler(message, keywords, producer):
    commit = parse_subscribe_repos_message(message)
    if not isinstance(commit, models.ComAtprotoSyncSubscribeRepos.Commit):
        return
    if not commit.blocks:
        return

    try:
        car = CAR.from_bytes(commit.blocks)

        for op in commit.ops or []:
            if op.action != "create" or not op.path.startswith("app.bsky.feed.post"):
                continue

            block = car.blocks.get(op.cid)
            if (
                not isinstance(block, dict)
                or block.get("$type") != "app.bsky.feed.post"
            ):
                continue

            text = block.get("text", "")
            text_lower = text.lower()

            matches = keyword_matches(text_lower, keywords)
            discard = matches is None or len(matches) == 0
            # if discard:
            #     continue

            # unpack matches
            mtopics = [m[0] for m in matches] if matches else None
            mkeywords = [m[1] for m in matches] if matches else None
            mlanguages = [m[2] for m in matches] if matches else None

            # serialize nested structures
            embed = block.get("embed", None)
            if embed is not None:
                embed = serialize_extra(embed)

            facets = block.get("facets", None)
            if facets is not None:
                facets = serialize_extra(facets)

            # Save matched posts to Kafka
            post = {
                "uri": f"at://{commit.repo}/{op.path}",
                "cid": str(op.cid),
                "author": commit.repo,
                "created_at": block.get("createdAt"),
                "text": text,
                "langs": block.get("langs", []),
                "embed": embed,
                "facets": facets,
                "reply": block.get("reply"),
                "keywords": list(dict.fromkeys(mkeywords)) if mkeywords else [],
                "topics": list(dict.fromkeys(mtopics)) if mtopics else [],
                "languages": list(dict.fromkeys(mlanguages)) if mlanguages else [],
            }

            post_key = f"{commit.repo}/{op.path}:" + post["cid"]

            # Send to Kafka
            producer.send("bluesky.firehose", key=post_key, value=post)

            if not discard:
                # Send to Kafka
                producer.send("bluesky.posts", key=post_key, value=post)

    except Exception as e:
        print(f"[ERR] Error in message handler: {e}")


def handler(context, event):
    global client
    """Main handler function to process events"""
    producer = context.producer
    keywords = context.keywords

    if not producer or not keywords:
        print("Producer or keywords not initialized. Can not start")
        return

    if client is not None:
        return "Already running"

    handle = lambda msg: on_message_handler(msg, keywords, producer)

    try:
        # start collection
        client = FirehoseSubscribeReposClient()
        client.start(handle)

    except Exception as e:
        print(f"Error with client: {e}")
        if client:
            try:
                client.stop()
                client = None
            except Exception as er:
                print(f"Error closing the client, dropping: {er}")
                client = None

        # wait to stagger the restart
        time.sleep(DELAY)
        handler(context, event)

    return "Running"
