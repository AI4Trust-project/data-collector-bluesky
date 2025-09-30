import json
import os
import uuid
from datetime import datetime, timedelta, timezone
import re
from multiformats import CID

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

def decode_embed_link(encoded_ref, author_did, fmt):
    """
    Decode an embedded reference (image or video) into a usable URL.

    Args:
        encoded_ref: CID reference (bytes for video, str for image).
        author_did: The author's DID string.
        fmt (str): Either "image" or "video".

    Returns:
        str or None: A URL to the media resource, or None if decoding fails.
    """
    if fmt == "image":
        try:
            if not isinstance(encoded_ref, (str, bytes)):
                raise TypeError(f"Expected str or bytes, got {type(encoded_ref)}")

            ref_decoded = CID.decode(encoded_ref)
            return f"https://cdn.bsky.app/img/feed_fullsize/plain/{author_did}/{ref_decoded}@jpeg"

        except Exception as e:
            print(f"[!] Failed to decode image ref: {e}")
            return None

    if fmt == "video":
        try:
            if not isinstance(encoded_ref, bytes):
                raise TypeError(f"Expected bytes, got {type(encoded_ref)}")

            cid_obj = CID.decode(encoded_ref)
            cid_base32 = cid_obj.encode("base32")
            return f"https://video.bsky.app/watch/{author_did}/{cid_base32}/playlist.m3u8"

        except Exception as e:
            print(f"[!] Failed to decode video ref: {e}")
            return None

    raise ValueError(f"Unsupported format: {fmt}")

def process_post(block, commit, op, keywords):
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



    embed = block.get("embed", None)
    images_links = None
    video_link = None
    author_did = commit.repo
    # serialize nested structures
    if (embed is not None) and (isinstance(embed, dict)):
        embed_type = embed.get("$type")

        if embed_type == "app.bsky.embed.images":
            images_links = []
            for image in embed["images"]:
                try:
                    ref = image.get("image", {}).get("ref")
                    if ref:
                        images_links.append(decode_embed_link(ref, author_did, "image"))
                except Exception as e:
                    print(f"[!] Failed to decode image: {e}")
        elif embed_type == "app.bsky.embed.video":
            print("Videos")
            try:
                ref = embed.get("video", {}).get("ref")
                if ref:
                    video_link = decode_embed_link(ref, author_did, "video")
            except Exception as e:
                print(f"[!] Failed to decode video: {e}")


        embed = serialize_extra(embed)

    facets = block.get("facets", None)
    if facets is not None:
        facets = serialize_extra(facets)


    reply_full = block.get("reply", None)
    root_cid = None
    parent_cid = None
    reply_to_store = None

    if reply_full is not None:
        root_cid = reply_full["root"]["cid"]
        parent_cid = reply_full["parent"]["cid"]
        reply_to_store = {
            "root": {
                "cid": root_cid,
                "uri": reply_full["root"]["cid"],
            },
            "parent": {
                "cid": parent_cid,
                "uri": reply_full["parent"]["cid"],
            }
        }

    # Save matched posts to Kafka
    post = {
        "uri": f"at://{commit.repo}/{op.path}",
        "cid": str(op.cid),
        "author": commit.repo,
        "created_at": block.get("createdAt"),
        "text": text,
        "langs": block.get("langs", []),
        "embed": embed,
        "images_links": images_links,
        "video_link": video_link,
        "facets": facets,
        "reply": reply_to_store,
        "root_cid": root_cid,
        "parent_cid": parent_cid,
        "keywords": list(dict.fromkeys(mkeywords)) if mkeywords else [],
        "topics": list(dict.fromkeys(mtopics)) if mtopics else [],
        "languages": list(dict.fromkeys(mlanguages)) if mlanguages else [],
    }

    return post, discard


def process_like(block, commit, op):
    return {
        "type": "app.bsky.feed.like",
        "author": commit.repo,
        "subject.cid": str(block.get("subject", {}).get("cid", "")),
        "subject.uri": block.get("subject", {}).get("uri", ""),
        "via.cid": str(block.get("via", {}).get("cid", "")) if block.get("via") else None,
        "via.uri": block.get("via", {}).get("uri", "") if block.get("via") else None,
        "createdAt": block.get("createdAt") or datetime.utcnow().isoformat(),
    }

def process_repost(block, commit, op):
    return {
        "type": "app.bsky.feed.repost",
        "author": commit.repo,
        "subject.cid": str(block.get("subject", {}).get("cid", "")) if block.get("subject") else None,
        "subject.uri": block.get("subject", {}).get("uri", "") if block.get("subject") else None,
        "via.cid": str(block.get("via", {}).get("cid", "")) if block.get("via") else None,
        "via.uri": block.get("via", {}).get("uri", "") if block.get("via") else None,
        "createdAt": block.get("createdAt") or datetime.utcnow().isoformat(),
    }

def on_message_handler(message, keywords, producer):
    commit = parse_subscribe_repos_message(message)
    if not isinstance(commit, models.ComAtprotoSyncSubscribeRepos.Commit):
        return
    if not commit.blocks:
        return

    try:
        car = CAR.from_bytes(commit.blocks)

        for op in commit.ops or []:
            if op.action != "create" or not op.path.startswith(
                ("app.bsky.feed.post", "app.bsky.feed.like", "app.bsky.feed.repost")
            ):
                continue

            block = car.blocks.get(op.cid)
            record_type = block.get("$type")
            if (
                not isinstance(block, dict)
                or record_type not in [
                    "app.bsky.feed.post",
                    "app.bsky.feed.like",
                    "app.bsky.feed.repost"
                ]
            ):
                continue
            
            if record_type == "app.bsky.feed.post":
                post, discard = process_post(block, commit, op)
                post_key = f"{commit.repo}/{op.path}:" + post["cid"]

                # Send to Kafka
                producer.send("bluesky.firehose", key=post_key, value=post)

                if not discard:
                    # Send to Kafka
                    producer.send("bluesky.posts", key=post_key, value=post)
            elif record_type == "app.bsky.feed.like":
                like = process_like(block, commit, op)
                like_key = f"{commit.repo}/{op.path}:" + like["subject.cid"]
                # TODO producer.send("bluesky.likes", key=like_key, value=like)

            elif record_type == "app.bsky.feed.repost":
                repost = process_repost(block, commit, op)
                repost_key = f"{commit.repo}/{op.path}:" + repost["subject.cid"]
                # TODO producer.send("bluesky.reposts", key=repost_key, value=repost)


            

            

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
