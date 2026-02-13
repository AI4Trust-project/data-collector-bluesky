from itertools import islice
import pandas as pd
import numpy as np
import os
from trino.dbapi import connect
from trino.exceptions import TrinoQueryError
import datetime as dt
import json
import requests

from kafka import KafkaProducer
import uuid
import types
import re

import urllib.parse
import urllib.request


API_KEY = os.environ.get("API_KEY")
API_URL = os.environ.get("API_URL")
TRINO_USER = os.environ.get("TRINO_USER")
TRINO_HOST = os.environ.get("TRINO_HOST")
MAX_MSG = 100000
TOP_N = 350
BSKY_API_ENDPOINT = "https://public.api.bsky.app/xrpc/app.bsky.feed.getPosts"

LANGUAGE_CODES = {
    "english": "EN",
    "french": "FR",
    "spanish": "ES",
    "german": "DE",
    "greek": "EL",
    "italian": "IT",
    "polish": "PL",
    "romanian": "RO",
}


# strict set of labels to filter out
EXCLUDED_LABELS = {
    # 1. Official Global Content Labels
    "porn",  # Images, 18+ only
    "sexual",  # Less intense sexual content
    "graphic-media",  # Violence / gore
    "nudity",  # Artistic/non-sexual nudity
    # 2. Official Global System Labels
    "!no-unauthenticated",  # Inaccessible to logged-out users (often used for adult content)
    "!hide",  # Generic warning, filters content from listings
    "!warn",  # Generic warning, implies sensitive content
    # 3. Common / Standard Values
    "spam",  # Explicitly mentioned in docs as a standard label
    "harassment",  # Mentioned as a common custom label definition
    # 4. Common Custom/Legacy Labels
    "nsfw",  # Common custom label
    "gore",  # Often mapped to graphic-media, but explicitly listed as an example value
    "sexual-figurative",
}


# ------------------------
# TODO Dummy language detector to replace with the one used in AI4Trust as language from bluesky is not reliable
# ------------------------
def detect_language(post):
    languages = post.get("languages", [])
    if languages is None or len(languages) == 0:
        return None

    # favor English
    if "English" in languages:
        return "EN"

    return LANGUAGE_CODES.get(languages[0].lower(), None)


def fetch_posts(conn, window_start, window_end):

    try:
        cur = conn.cursor()
        query = """
            WITH ranked_posts AS (
                SELECT *,
                       ROW_NUMBER() OVER (PARTITION BY cid ORDER BY created_at) as rn
                FROM bluesky.posts
                WHERE from_iso8601_timestamp(created_at) 
                    BETWEEN from_unixtime(?) and from_unixtime(?) 
                    AND cardinality(keywords) > 0
            )
            SELECT * 
            FROM ranked_posts
            WHERE rn = 1
            LIMIT ?    
        """
        rows = cur.execute(
            query, [int(window_start), int(window_end), MAX_MSG]
        ).fetchall()
        columns = [c[0] for c in cur.description]
        df_raw = pd.DataFrame(
            rows,
            columns=columns,
        )
        # TODO here it should also be decided what keywords we send for now, all of them, just subword, fullword,...
        # TODO Additionally, here we should filter out posts that have already been sent, how do we want to do this?

        return df_raw
    finally:
        cur.close()


def fetch_likes(conn, window_start, window_end):

    try:
        cur = conn.cursor()
        query = """
        WITH lk AS (
          SELECT
            subject.cid as cid
          from
            bluesky.likes
          where
            _timestamp between ?
            and ?
        ),
        posts AS (
          SELECT
            DISTINCT(f.cid) as cid
          FROM
            bluesky.posts as f
            JOIN lk on f.cid = lk.cid
          WHERE
            from_iso8601_timestamp(created_at) BETWEEN from_unixtime(?)
            and from_unixtime(?)
            AND cardinality(keywords) > 0
        )
        SELECT
          l.subject.cid as cid,
          count(*) as nr_likes
        from
          bluesky.likes as l
          join posts as p on l.subject.cid = p.cid
        group by
          l.subject.cid
        """
        rows = cur.execute(
            query,
            [
                int(window_start * 1000),
                int(window_end * 1000),
                int(window_start),
                int(window_end),
            ],
        ).fetchall()
        columns = [c[0] for c in cur.description]
        df_raw = pd.DataFrame(
            rows,
            columns=columns,
        )

        return df_raw
    finally:
        cur.close()


def fetch_reposts(conn, window_start, window_end):

    try:
        cur = conn.cursor()
        query = """
        WITH lk AS (
          SELECT
            subject.cid as cid
          from
            bluesky.reposts
          where
            _timestamp between ?
            and ?
        ),
        posts AS (
          SELECT
            DISTINCT(f.cid) as cid
          FROM
            bluesky.posts as f
            JOIN lk on f.cid = lk.cid
          WHERE
            from_iso8601_timestamp(created_at) BETWEEN from_unixtime(?)
            and from_unixtime(?)
            AND cardinality(keywords) > 0
        )
        SELECT
          l.subject.cid as cid,
          count(*) as nr_reposts
        from
          bluesky.reposts as l
          join posts as p on l.subject.cid = p.cid
        group by
          l.subject.cid
        """
        rows = cur.execute(
            query,
            [
                int(window_start * 1000),
                int(window_end * 1000),
                int(window_start),
                int(window_end),
            ],
        ).fetchall()
        columns = [c[0] for c in cur.description]
        df_raw = pd.DataFrame(
            rows,
            columns=columns,
        )
        # TODO here it should also be decided what keywords we send for now, all of them, just subword, fullword,...
        # TODO Additionally, here we should filter out posts that have already been sent, how do we want to do this?

        return df_raw
    finally:
        cur.close()


def fetch_engagement(conn, window_start, window_end):

    df_likes = fetch_likes(conn, window_start, window_end)
    df_reposts = fetch_reposts(conn, window_start, window_end)

    engagement_df = pd.merge(df_likes, df_reposts, on="cid", how="outer")
    engagement_df["nr_likes"] = engagement_df["nr_likes"].fillna(0).astype(int)
    engagement_df["nr_reposts"] = engagement_df["nr_reposts"].fillna(0).astype(int)

    return engagement_df


def compute_engagement_ranking(posts_df, engagement_df):
    posts_df = posts_df.merge(engagement_df, left_on="cid", right_on="cid", how="left")
    posts_df["nr_likes"] = posts_df["nr_likes"].fillna(0).astype(int)
    posts_df["nr_reposts"] = posts_df["nr_reposts"].fillna(0).astype(int)

    likes_sorted = np.sort(posts_df["nr_likes"].values)
    reposts_sorted = np.sort(posts_df["nr_reposts"].values)

    def P_ge(values_sorted, x):
        return (
            np.sum(values_sorted >= x) / len(values_sorted)
            if len(values_sorted) > 0
            else 0
        )

    engagement_scores = []
    for _, row in posts_df.iterrows():
        p_likes = P_ge(likes_sorted, row["nr_likes"])
        p_reposts = P_ge(reposts_sorted, row["nr_reposts"])
        score = (1 / p_likes if p_likes > 0 else 0) + (
            1 / p_reposts if p_reposts > 0 else 0
        )
        engagement_scores.append(score)

    posts_df["engagement_score"] = engagement_scores
    top_posts = posts_df.sort_values("engagement_score", ascending=False)
    return top_posts


def bluesky_uri_to_url(uri):
    """
    Transform a Bluesky `at://did:plc:.../app.bsky.feed.post/...` URI
    into a web URL like `https://bsky.app/profile/.../post/...`
    """
    if not isinstance(uri, str):
        return None

    # Match user DID and post CID
    match = re.match(r"at://([^/]+)/app\.bsky\.feed\.post/([^/]+)", uri)
    if match:
        user_did, post_cid = match.groups()
        return f"https://bsky.app/profile/{user_did}/post/{post_cid}"
    return None


def convert_to_at_uri(web_url):
    """Converts a bsky.app web URL to an AT Protocol URI."""
    if not web_url:
        return None
    match = re.search(r"profile/([^/]+)/post/([^/]+)", web_url)
    if match:
        return f"at://{match.group(1)}/app.bsky.feed.post/{match.group(2)}"
    return None


def recursive_label_scan(obj, path="root", depth=0, max_depth=50):
    """
    Recursively traverses ANY JSON structure (dict or list) to find banned labels.

    Args:
        obj: The dictionary or list to scan.
        path: A string trace of where we are in the JSON (for debugging).
        depth: Current recursion depth.
        max_depth: Safety limit to prevent infinite recursion.

    Returns:
        (bool, str): (True if bad label found, Reason string)
    """
    # Guardrail: Stop if too deep to prevent crashes
    if depth > max_depth:
        return False, None

    # Case 1: It's a Dictionary (Label, Post, Author, Embed, etc.)
    if isinstance(obj, dict):
        # A. Check for the smoking gun: A 'val' key with a banned string
        if "val" in obj and isinstance(obj["val"], str):
            val = obj["val"].lower()
            if val in EXCLUDED_LABELS:
                return True, f"Found banned label '{val}' at path: {path}"

        # B. Recurse into children
        for key, value in obj.items():
            # Optimization: Skip large binary blobs or irrelevant image data to save time
            if key in ["image", "thumb", "avatar", "banner"]:
                continue

            found, reason = recursive_label_scan(
                value, path=f"{path}.{key}", depth=depth + 1
            )
            if found:
                return True, reason

    # Case 2: It's a List (e.g. list of labels, list of facets)
    elif isinstance(obj, list):
        for i, item in enumerate(obj):
            found, reason = recursive_label_scan(
                item, path=f"{path}[{i}]", depth=depth + 1
            )
            if found:
                return True, reason

    # Case 3: Safe types (str, int, bool, None)
    return False, None


def fetch_posts_batch(uris):
    """Fetches a batch of posts from the public API. Bluesky has a rate limit of 25 uris per call"""
    if not uris:
        return []

    def batched(iterable, chunk_size):
        iterator = iter(iterable)
        while chunk := tuple(islice(iterator, chunk_size)):
            yield chunk

    res = []
    # Note: Public API allows max 25 URIs per request
    for uri_batch in batched(uris, 25):
        resp = requests.get(BSKY_API_ENDPOINT, params={"uris": uri_batch})
        resp.raise_for_status()
        res.extend(resp.json().get("posts", []))

    return res


def prepare(post):
    detected_language = detect_language(post)
    image_urls = post.get("image_urls", None)
    image_url = None

    # main image link logic
    if isinstance(image_urls, list) and len(image_urls) > 0:
        image_url = str(image_urls[0])
    elif post.get("video_thumbnail_url") is not None:
        image_url = post["video_thumbnail_url"]
        image_urls = [image_url]
    elif post.get("external_link_thumbnail_url") is not None:
        image_url = post["external_link_thumbnail_url"]
        image_urls = [image_url]

    msg = {
        "id": post["cid"],
        "data_owner": "FBK",
        "text": str(post["text"]),
        "language": str(detected_language),
        "url": bluesky_uri_to_url(post["uri"]),
        "image_url": image_url,
        "image_urls": image_urls,
        "video_url": post.get("video_url", None),
        "publish_time": post.get("created_at"),
        "created_at": post.get("created_at"),
        "topic": (post.get("topics")[0] if post.get("topics") else None),
        "likes": int(post.get("nr_likes", 0)),
        "reposts": int(post.get("nr_reposts", 0)),
    }

    return msg


def init_context(context):
    producer = KafkaProducer(
        bootstrap_servers=[os.environ.get("KAFKA_BROKER")],
        key_serializer=lambda x: x.encode("utf-8"),
        value_serializer=lambda x: json.dumps(x).encode("utf-8"),
    )
    setattr(context, "producer", producer)


def handler(context, event):
    body = event.body.decode("utf-8")
    producer = context.producer
    now = dt.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    ncount = TOP_N

    if body:
        parameters = json.loads(body)
        if "date" in parameters:
            now = dt.datetime.strptime(parameters["date"], "%Y-%m-%d").replace(
                hour=0, minute=0, second=0, microsecond=0
            )
        if "top_n" in parameters:
            ncount = parameters["top_n"]

    try:
        conn = connect(
            host=TRINO_HOST,
            user=TRINO_USER,
            catalog="iceberg",
        )

        window_end = (now).timestamp()
        window_start = (now - dt.timedelta(days=1)).timestamp()

        context.logger.debug(
            f"Fetch {ncount} messages for {window_start} - {window_end}..."
        )
        df = fetch_posts(conn, window_start, window_end)
        if not df.empty:
            context.logger.debug(f"Fetch engagement data...")
            engagement_df = fetch_engagement(conn, window_start, window_end)
            # fetch double N to account for possible filtering later
            top_posts = compute_engagement_ranking(df, engagement_df).head(ncount * 2)

            context.logger.debug(
                f"Prepare and send {ncount} from {len(top_posts)} posts..."
            )
            count = 0
            # for _, post in top_posts.iterrows():
            uris = top_posts["uri"].tolist()
            posts_data = fetch_posts_batch(uris)
            raws = [{"uri": x["uri"], "raw": x} for x in posts_data]
            rdf = pd.DataFrame.from_records(raws)
            posts = top_posts.merge(rdf, how="left", on="uri")
            for _, post in posts.iterrows():
                try:
                    # filter: The Scanner looks everywhere: Author, Record, Embeds, Facets
                    is_bad, reason = recursive_label_scan(post["raw"])
                    if is_bad:
                        context.logger.debug(
                            f"Skipping post {post['cid']} due to filtering: {reason}"
                        )
                        continue

                    message = prepare(post)
                    context.logger.debug("Send message " + message["id"])
                    data = json.dumps(message)
                    count += 1
                    if count > ncount:
                        break

                    try:
                        # send to kafka
                        value = json.loads(data)
                        producer.send(
                            "pipe.bluesky",
                            key=message["id"],
                            value=value,
                        )
                    except Exception as e:
                        context.logger.error(f"Error writing to Kafka: {e}")

                    if API_URL is not None:
                        api = API_URL.split(",")
                        for apiurl in api:
                            # send
                            req = urllib.request.Request(
                                apiurl, data=data.encode("utf-8")
                            )
                            req.add_header("Content-Type", "application/json")
                            req.add_header("X-API-KEY", API_KEY)
                            req.get_method = lambda: "POST"

                            try:
                                with urllib.request.urlopen(req) as f:
                                    response = f.read().decode("utf-8")

                            except Exception as e:
                                context.logger.error(f"Error calling API {apiurl}: {e}")

                except Exception as e:
                    context.logger.error(f"Error with processing: {e}")

            context.logger.debug(f"Done.")
            return context.Response(
                body=f"Done",
                headers={},
                content_type="text/plain",
                status_code=200,
            )

    except Exception as e:
        context.logger.error(f"Error with processing: {e}")
    finally:
        conn.close()
