from pathlib import Path
from typing import Union
import pandas as pd
import praw
# from gcs_rw import write_to_gcs
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

@task(tags="extract reddit posts", log_prints=True)
def extract_posts(subreddit_name: str) -> pd.DataFrame:
    all_posts_list = list()
    reddit = praw.Reddit(
        client_id="r8QZQcEfIwMVfANMyk7zDQ",
        client_secret="ovMa8feR9Ne93LQ-SXvWAo3sZRMuMg",
        user_agent="project 3 by tranvanchao",
    )

    subreddit = reddit.subreddit(subreddit_name)
    for submission in subreddit.top(time_filter="day"):
        # id is not enough, also must take into account post_url
            print("found new posts")
            author = submission.author
            author_flair_text = submission.author_flair_text
            clicked = submission.clicked
            distinguished = submission.distinguished
            edited = submission.edited
            post_id = submission.id
            is_original_content = submission.is_original_content
            locked = submission.locked
            name = submission.name
            title = submission.title
            text = submission.selftext
            num_comments = submission.num_comments
            score = submission.score
            url = submission.url
            saved = submission.saved
            created_at = submission.created_utc
            over_18 = submission.over_18
            spoiler = submission.spoiler
            stickied = submission.stickied
            upvote_ratio = submission.upvote_ratio

            dict_post_preview = {
                "author": str(author),
                "author_flair_text": str(author_flair_text),
                "clicked": bool(clicked),
                "distinguished": bool(distinguished),
                "edited": bool(edited),
                "post_id": str(post_id),
                "is_original_content": bool(is_original_content),
                "locked": bool(locked),
                "post_fullname": str(name),
                "post_title": str(title),
                "post_text": str(text),
                "num_comments": str(num_comments),
                "post_score": float(score),
                "post_url": str(url),
                "saved": bool(saved),
                "created_at": float(created_at),
                "over_18": bool(over_18),
                "spoiler": bool(spoiler),
                "stickied": bool(stickied),
                "upvote_ratio": float(upvote_ratio),
            }

            all_posts_list.append(dict_post_preview)

    df_raw = pd.DataFrame(all_posts_list)
    return df_raw

# @task(log_prints=True)
def write_to_gcs(
    local_path: Union[Path, str], gcs_bucket_path: Union[Path, str]) -> None:
    gcs_bucket_block = GcsBucket.load("zc-gcs")
    gcs_bucket_block.upload_from_path(from_path=local_path, to_path=gcs_bucket_path)
    print(f"succesfully uploaded to {gcs_bucket_path}")

@task(log_prints=True)
def clean_df(df: pd.DataFrame) -> pd.DataFrame:
    df["created_at"] = pd.to_datetime(df["created_at"], unit="s")
    return df


@task(log_prints=True)
def concat_df(new_df: pd.DataFrame, df: pd.DataFrame) -> pd.DataFrame:
    new_df = new_df.set_index('post_id')  # Đặt cột id làm cột index trong df1
    df = df.set_index('post_id')  # Đặt cột id làm cột index trong df2

    # Cập nhật score trong df1 dựa trên df2
    df.update(new_df)

    # Thêm những dữ liệu từ df2 có id không nằm trong df1 vào df1
    df = pd.concat([df,new_df[~new_df.index.isin(df.index)]])

    # Đặt lại cột id thành cột dữ liệu
    df = df.reset_index()

    return df


@task(log_prints=True)
def write_local_and_to_gcs(df: pd.DataFrame) -> None:
    local_path = Path("data/rde.parquet")
    local_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(local_path, compression="gzip")
    write_to_gcs(local_path=local_path, gcs_bucket_path=local_path)


@flow()
def scrape_reddit() -> None:
    path = extract_from_gcs()
    df = transform(path)
    df_raw = extract_posts(
        subreddit_name="dataengineering+datascience+dataanalysis")
    df_medium = clean_df(df_raw)
    final_df = concat_df(df_medium, df)    
    # delete dups because column "post_url" is being problematic
    # concatenated_df.drop_duplicates(subset=["post_url"], keep="last", inplace=True)
    write_local_and_to_gcs(final_df)


if __name__ == "__main__":
    scrape_reddit()