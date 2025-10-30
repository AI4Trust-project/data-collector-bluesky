import json
import os
from datetime import datetime
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

def _iceberg_json_default(value):
    if isinstance(value, datetime):
        return value.isoformat()
    elif isinstance(value, set):
        return list(value)
    else:
        return repr(value)


def get_keywords(conn):
    """Get keywords from DB and compile regex patterns based on match_type"""
    cur = None
    data = []

    try:
        cur = conn.cursor()
        query = """
            SELECT keyword, topic, language, match_type
            FROM bluesky.search_keywords
            WHERE keyword_id < 3000 ORDER BY keyword_id
        """
        cur.execute(query)
        rows = cur.fetchall()

        for keyword, topic, language, match_type in rows:
            if match_type == "sub_word":
                pattern = re.escape(keyword).replace(r"\*", ".*")
            else:  # full_word
                pattern = r"\b" + re.escape(keyword).replace(r"\*", ".*") + r"\b"

            regex = re.compile(pattern, re.IGNORECASE)
            data.append({
                "word": keyword,
                "regex": regex,
                "lang": language,
                "topic": topic,
                "match_type": match_type
            })

    except Exception as e:
        print("ERROR fetching keywords:", e)
    finally:
        if cur:
            cur.close()

    return data


def keyword_matches(text, keywords):
    """Filter text based on keywords"""
    match = []

    for k in keywords:
        if k["regex"].search(text):
            match.append({
                "keyword": k["word"],
                "language": k["lang"],
                "topic": k["topic"]
            })
    # Deduplicate
    keywords_list = list({m["keyword"] for m in match})
    languages = list({m["language"] for m in match})
    topics = list({m["topic"] for m in match})
    return {"keywords": keywords_list, "languages": languages, "topics": topics}


def init_context(context):
    # Fetch keywords from DB (with regex compiled)
    conn = psycopg2.connect(dbname=DBNAME, user=USER, password=PASSWORD, host=HOST)
    keywords = get_keywords(conn)
    conn.close()

    # init producer
    producer = KafkaProducer(
        bootstrap_servers=[os.environ.get("KAFKA_BROKER")],
        key_serializer=lambda x: x.encode("utf-8"),
        value_serializer=lambda x: json.dumps(x, default=_iceberg_json_default).encode("utf-8"),
    )

    if not producer or not keywords:
        print("Producer or keywords not initialized. Cannot start")
        return

    setattr(context, "producer", producer)
    setattr(context, "keywords", keywords)

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
            return (
                f"https://video.bsky.app/watch/{author_did}/{cid_base32}/playlist.m3u8",
                f"https://video.bsky.app/watch/{author_did}/{cid_base32}/thumbnail.jpg"
            )
        except Exception as e:
            print(f"[!] Failed to decode video ref: {e}")
            return None, None

    raise ValueError(f"Unsupported format: {fmt}")


def process_post(block, commit, op, keywords):
    text = block.get("text", "").lower()
    matches = keyword_matches(text, keywords)

    # Embed handling
    embed = block.get("embed")
    images_links = None
    video_link = None
    external_link_url = None
    external_link_image_link = None
    video_thumbnail_link = None
    author_did = commit.repo

    if embed and isinstance(embed, dict):
        embed_type = embed.get("$type")

        if embed_type == "app.bsky.embed.images":
            images_links = []
            for image in embed.get("images", []):
                ref = image.get("image", {}).get("ref")
                if ref:
                    images_links.append(decode_embed_link(ref, author_did, "image"))
        elif embed_type == "app.bsky.embed.video":
            ref = embed.get("video", {}).get("ref")
            if ref:
                video_link, video_thumbnail_link = decode_embed_link(ref, author_did, "video")
        elif embed_type == "app.bsky.embed.external":
            external_link_url = embed.get("external", {}).get("uri")
            thumb_ref = embed.get("external", {}).get("thumb", {}).get("ref")
            if thumb_ref:
                external_link_image_link = decode_embed_link(thumb_ref, author_did, "image")
        embed =  serialize_extra(embed)

    # Deal with facets
    facets = block.get("facets")
    if facets:
        facets = serialize_extra(facets)

    # Reply info
    reply_full = block.get("reply")
    root_cid = None
    parent_cid = None
    reply_to_store = None
    if reply_full:
        root_cid = reply_full["root"]["cid"]
        parent_cid = reply_full["parent"]["cid"]
        reply_to_store = {
            "root": {"cid": root_cid, "uri": reply_full["root"]["uri"]},
            "parent": {"cid": parent_cid, "uri": reply_full["parent"]["uri"]}
        }

    return {
        "uri": f"at://{commit.repo}/{op.path}",
        "cid": str(op.cid),
        "author": commit.repo,
        "created_at": block.get("createdAt"),
        "text": block.get("text", ""),
        "langs": block.get("langs", []),
        "embed": embed,
        "images_links": images_links,
        "video_link": video_link,
        "video_thumbnail_link": video_thumbnail_link,
        "external_link_url": external_link_url,
        "external_link_image_link": external_link_image_link,
        "facets": facets,
        "reply": reply_to_store,
        "root_cid": root_cid,
        "parent_cid": parent_cid,
        "keywords": matches["keywords"],
        "languages": matches["languages"],
        "topics": matches["topics"]
    }


def process_like(block, commit, op):
    return {
        "type": "app.bsky.feed.like",
        "author": commit.repo,
        "subject.cid": str(block.get("subject", {}).get("cid", "")),
        "subject.uri": block.get("subject", {}).get("uri", ""),
        "via.cid": str(block.get("via", {}).get("cid", "")) if block.get("via") else None,
        "via.uri": block.get("via", {}).get("uri", "") if block.get("via") else None,
        "createdAt": block.get("createdAt"),
    }

def process_repost(block, commit, op):
    return {
        "type": "app.bsky.feed.repost",
        "author": commit.repo,
        "subject.cid": str(block.get("subject", {}).get("cid", "")) if block.get("subject") else None,
        "subject.uri": block.get("subject", {}).get("uri", "") if block.get("subject") else None,
        "via.cid": str(block.get("via", {}).get("cid", "")) if block.get("via") else None,
        "via.uri": block.get("via", {}).get("uri", "") if block.get("via") else None,
        "createdAt": block.get("createdAt"),
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
            record_type = block.get("$type") if isinstance(block, dict) else None
            if record_type not in ["app.bsky.feed.post", "app.bsky.feed.like", "app.bsky.feed.repost"]:
                continue

            if record_type == "app.bsky.feed.post":
                post = process_post(block, commit, op, keywords)
                post_key = f"{commit.repo}/{op.path}:{post['cid']}"
                producer.send("bluesky.firehose", key=post_key, value=post)

            elif record_type == "app.bsky.feed.like":
                like = process_like(block, commit, op)
                like_key = f"{commit.repo}/{op.path}:{like.get('subject.cid') or 'no-cid'}"
                # TODO producer.send("bluesky.likes", key=like_key, value=like)

            elif record_type == "app.bsky.feed.repost":
                repost = process_repost(block, commit, op)
                repost_key = f"{commit.repo}/{op.path}:{repost.get('subject.cid') or 'no-cid'}"
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
