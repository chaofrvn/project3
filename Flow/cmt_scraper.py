from pathlib import Path
from typing import Union
import pandas as pd
import praw
import prawcore
# from gc_funcs.reader_writer import (
#     get_comments_from_gcs,
#     get_posts_from_gcs,
#     write_to_gcs,
# )
from praw.models import MoreComments
from prefect import flow, task
# from prefect.blocks.system import Secret
from prefect_gcp.cloud_storage import GcsBucket


@task(retries=3)
def extract_from_gcs() -> Path:
    """Download reddit data from GCS"""
    gcs_path = f'data/rde.parquet'
    gcs_block = GcsBucket.load('zc-gcs')
    gcs_block.get_directory(from_path=gcs_path, local_path='./')
    return Path(gcs_path)

@task(log_prints=True)
def transform(path: Path) -> pd.DataFrame:
    """Data cleaning example"""
    df = pd.read_parquet(path)
    return df

@task(retries=3)
def extract_cmt_from_gcs() -> Path:
    """Download reddit data from GCS"""
    gcs_path = f'data/cmt_rd.parquet'
    gcs_block = GcsBucket.load('zc-gcs')
    gcs_block.get_directory(from_path=gcs_path, local_path='./')
    return Path(gcs_path)

@task(log_prints=True)
def transform_cmt(path: Path) -> pd.DataFrame:
    """Data cleaning example"""
    df = pd.read_parquet(path)
    return df

@task(tags="extract reddit comments", log_prints=True)
def extract_comments(
    df_posts_from_bucket: pd.DataFrame
    , df_comments_from_bucket: pd.DataFrame
) -> pd.DataFrame:
    comment_id_from_comments = set(df_comments_from_bucket["comment_id"].to_list())
    df_comments_from_bucket = df_comments_from_bucket.loc[
        df_comments_from_bucket["post_url"].notnull()
    ]
    posts_url_from_comments = set(df_comments_from_bucket["post_url"].to_list())
    posts_url_from_posts = set(df_posts_from_bucket["post_url"].to_list())
    all_comments_list = list()
    reddit = praw.Reddit(
        client_id="r8QZQcEfIwMVfANMyk7zDQ",
        client_secret="ovMa8feR9Ne93LQ-SXvWAo3sZRMuMg",
        user_agent="project 3 by tranvanchao",
    )
    for post_url in posts_url_from_posts:
        if post_url not in posts_url_from_comments:
            try:
                submission = reddit.submission(url=post_url)
                for top_level_comment in submission.comments:
                    # some posts urls are deleted, so it is not enough to check
                    post_url
                    if (
                        isinstance(top_level_comment, MoreComments)
                        or top_level_comment.id in comment_id_from_comments
                    ):
                        print(
                            "comment already in dataset or comment with more comments structure"  # noqa: E501
                        )
                        continue
                    print("new comments found")
                    author = top_level_comment.author
                    comment_id = top_level_comment.id
                    submission_url = top_level_comment.submission.url
                    body = top_level_comment.body
                    created_at = top_level_comment.created_utc
                    distinguished = top_level_comment.distinguished
                    edited = top_level_comment.edited
                    is_submitter = top_level_comment.is_submitter
                    post_id = top_level_comment.link_id
                    link_comment = top_level_comment.permalink
                    score = top_level_comment.score

                    dict_post_preview = {
                        "author": str(author),
                        "comment_id": str(comment_id),
                        "post_url": str(submission_url),
                        "body": str(body),
                        "created_at": float(created_at),
                        "distinguished": bool(distinguished),
                        "edited": bool(edited),
                        "is_author_submitter": bool(is_submitter),
                        "post_id": str(post_id),
                        "link_comment": str(link_comment),
                        "comment_score": float(score),
                    }
                    all_comments_list.append(dict_post_preview)
            except (praw.exceptions.InvalidURL, prawcore.exceptions.NotFound) as e:
                """
                Some url posts are images, or gifs or maybe the post was deleted
                """
                print(e)
                continue

    df_comments_raw = pd.DataFrame(all_comments_list)
    if df_comments_raw.empty:
        raise Exception("no new data available")
    return df_comments_raw


@task(log_prints=True)
def clean_df(df: pd.DataFrame) -> pd.DataFrame:
    df["created_at"] = pd.to_datetime(df["created_at"], unit="s")
    # print("test----------------------")
    # print(df.iloc[0])
    return df


@task(log_prints=True)
def concat_df(
    new_df: pd.DataFrame, df_comments_from_bucket: pd.DataFrame
) -> pd.DataFrame:
    concatenated_df = pd.concat([df_comments_from_bucket, new_df])
    return concatenated_df

def write_to_gcs(
    local_path: Union[Path, str], gcs_bucket_path: Union[Path, str]) -> None:
    gcs_bucket_block = GcsBucket.load("zc-gcs")
    gcs_bucket_block.upload_from_path(from_path=local_path, to_path=gcs_bucket_path)
    print(f"succesfully uploaded to {gcs_bucket_path}")

@task(log_prints=True)
def write_local_and_to_gcs(df: pd.DataFrame) -> None:
    local_path = Path("data/cmt_rd.parquet")
    local_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(local_path, compression="gzip")
    write_to_gcs(local_path=local_path, gcs_bucket_path=local_path)


@flow(log_prints=True)
def scrape_reddit_comments() -> None:
    path = extract_from_gcs()
    df_posts_from_bucket = transform(path)
    path_cmt = extract_cmt_from_gcs()
    df_comments_from_bucket = transform_cmt(path_cmt)
    df_raw = extract_comments(df_posts_from_bucket, df_comments_from_bucket)
    # df_raw = extract_comments(df_posts_from_bucket)

    new_df = clean_df(df_raw)
    concatenated_df = concat_df(new_df, df_comments_from_bucket)
    # keep the last comment
    concatenated_df.drop_duplicates(
        subset=["author", "body", "created_at", "comment_id", "post_id"],
        keep="last",
        inplace=True,
    )
    # write_local_and_to_gcs(new_df)

    write_local_and_to_gcs(concatenated_df)


if __name__ == "__main__":
    scrape_reddit_comments()