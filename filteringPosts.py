import pandas as pd
import numpy as np
import os
import psycopg2

DBNAME = os.environ.get("DATABASE_NAME")
USER = os.environ.get("DATABASE_USER")
PASSWORD = os.environ.get("DATABASE_PWD")
HOST = os.environ.get("DATABASE_HOST")

# ------------------------
# TODO Dummy language detector to replace with the one used in AI4Trust as language from bluesky is not reliable
# ------------------------
def detect_language(text):
    return "en"

# ============================================================
# 1. Fetch posts from bluesky.posts
# ============================================================
def fetch_posts(conn, start_date, end_date):

    query = f"""
        SELECT *
        FROM bluesky.posts
        WHERE from_iso8601_timestamp(created_at) 
              BETWEEN TIMESTAMP '{start_date} 00:00:00'
                  AND TIMESTAMP '{end_date} 23:59:59'
          AND cardinality(keywords) > 0
    """
    #TODO do we have to query bluesky.firehose or bluesky.posts? In the second case we should remove the filter of keywords. I am saying it because I checked bluesky.posts and some info/columns were not available
    #TODO here it should also be decided what keywords we send for now, all of them, just subword, fullword,...
    #TODO Additionally, here we should filter out posts that have already been sent, how do we want to do this?

    posts = pd.read_sql(query, conn)

    return posts

def fetch_engagement(conn, post_ids, batch_size=500):
    if not post_ids:
        return pd.DataFrame(columns=["post_id", "n_likes", "n_reposts"])

    likes_list = []
    reposts_list = []

    for i in range(0, len(post_ids), batch_size): # TODO here I had to use batches because otherwise sql exploded in the platform
        batch = post_ids[i:i + batch_size]

        # Wrap strings in single quotes and join
        batch_str = ','.join(f"'{str(pid)}'" for pid in batch)

        query_likes = f"""
            SELECT subject.cid AS post_id, COUNT(*) AS n_likes
            FROM bluesky.likes
            WHERE subject.cid IN ({batch_str})
            GROUP BY subject.cid
        """
        likes_list.append(pd.read_sql(query_likes, conn))

        query_reposts = f"""
            SELECT subject.cid AS post_id, COUNT(*) AS n_reposts
            FROM bluesky.reposts
            WHERE subject.cid IN ({batch_str})
            GROUP BY subject.cid
        """
        reposts_list.append(pd.read_sql(query_reposts, conn))

    likes_df = pd.concat(likes_list, ignore_index=True)
    reposts_df = pd.concat(reposts_list, ignore_index=True)

    engagement_df = pd.merge(likes_df, reposts_df, on="post_id", how="outer")
    engagement_df["n_likes"] = engagement_df["n_likes"].fillna(0).astype(int)
    engagement_df["n_reposts"] = engagement_df["n_reposts"].fillna(0).astype(int)

    return engagement_df


# ============================================================
# 3. Rank posts by engagement
# ============================================================
def compute_engagement_ranking(posts_df, engagement_df, n):
    posts_df = posts_df.merge(engagement_df, left_on="cid", right_on="post_id", how="left")
    posts_df["n_likes"] = posts_df["n_likes"].fillna(0).astype(int)
    posts_df["n_reposts"] = posts_df["n_reposts"].fillna(0).astype(int)

    likes_sorted = np.sort(posts_df["n_likes"].values)
    reposts_sorted = np.sort(posts_df["n_reposts"].values)

    def P_ge(values_sorted, x):
        return np.sum(values_sorted >= x) / len(values_sorted) if len(values_sorted) > 0 else 0

    engagement_scores = []
    for _, row in posts_df.iterrows():
        p_likes = P_ge(likes_sorted, row["n_likes"])
        p_reposts = P_ge(reposts_sorted, row["n_reposts"])
        score = (1 / p_likes if p_likes > 0 else 0) + (1 / p_reposts if p_reposts > 0 else 0)
        engagement_scores.append(score)

    posts_df["engagement_score"] = engagement_scores
    top_posts = posts_df.sort_values("engagement_score", ascending=False).head(n)
    return top_posts

# ============================================================
# 4. Prepare posts to send to Fincons
# ============================================================


def bluesky_uri_to_url(uri):
    """
    Transform a Bluesky `at://did:plc:.../app.bsky.feed.post/...` URI
    into a web URL like `https://bsky.app/profile/.../post/...`
    """
    if not isinstance(uri, str):
        return None
    import re

    # Match user DID and post CID
    match = re.match(r"at://([^/]+)/app\.bsky\.feed\.post/([^/]+)", uri)
    if match:
        user_did, post_cid = match.groups()
        return f"https://bsky.app/profile/{user_did}/post/{post_cid}"
    return None

def prepare_posts_for_fincons(posts_df):
    posts_to_fincons = []

    for _, post in posts_df.iterrows():
        detected_language = detect_language(post["text"])
        image_urls = post.get("image_urls", None)
        image_url = None

        # main image link logic
        if isinstance(image_urls, list) and len(image_urls) > 0:
            image_url = image_urls[0]
        elif post.get("video_thumbnail_url") is not None:
            image_url = post["video_thumbnail_url"]
            image_urls = [image_url]
        elif post.get("external_link_thumbnail_url") is not None:
            image_url = post["external_link_thumbnail_url"]
            image_urls = [image_url]

        post_to_fincons = {
            "id": post["cid"],  #TODO is this id something internal or should it be the cid/uri from the post?
            "data_owner": "FBK",
            "text": str(post["text"]),
            "language": str(detected_language),
            "url": bluesky_uri_to_url(post["uri"]),
            "image_url": str(image_url),
            "image_urls": image_urls,
            "video_url": post.get("video_url", None),
            "publish_time": post.get("created_at"),
            "topic": post.get("topics")[0] if post.get("topics") else None, #TODO some posts can have more than one topic, how was this handled for telegram and youtube
            "likes": int(post.get("n_likes", 0)),
            "reposts": int(post.get("n_reposts", 0))
        }

        posts_to_fincons.append(post_to_fincons)

    return posts_to_fincons



# Dummy send function (replace with actual Fincons API call)
def send_posts_to_fincons(posts_to_send):
    print(f"Sending {len(posts_to_send)} posts to Fincons...")
    return 0

# ============================================================
# Example workflow
# ============================================================
if __name__ == "__main__":
    start_date = "2025-10-11"
    end_date = "2025-10-13" #TODO what range and how oftern should we actually send them posts, and how many?
    top_n = 1000

    conn = psycopg2.connect(dbname=DBNAME, user=USER, password=PASSWORD, host=HOST)
    posts_df = fetch_posts(conn, start_date, end_date)

    posts_to_fincons = []
    if not posts_df.empty:
        engagement_df = fetch_engagement(conn, posts_df["cid"].tolist())
        top_posts = compute_engagement_ranking(posts_df, engagement_df, top_n)
        posts_to_fincons = prepare_posts_for_fincons(top_posts)

    conn.close()
    send_posts_to_fincons(posts_to_fincons)