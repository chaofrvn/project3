import random
from collections import Counter
from pathlib import Path
from typing import Union

import matplotlib
import matplotlib.pyplot as plt
import nltk
import pandas as pd
import seaborn as sns
from prefect_gcp.cloud_storage import GcsBucket

# Lemmatizer helps to reduce words to the base form
from nltk.stem import WordNetLemmatizer

# This allows to create individual objects from a bog of words
from nltk.tokenize import word_tokenize
from prefect import flow, task
from wordcloud import WordCloud


def grey_color_func(
    word,  # noqa: ANN001, ARG001
    font_size,  # noqa: ANN001, ARG001
    position,  # noqa: ANN001, ARG001
    orientation,  # noqa: ANN001, ARG001
    random_state=None,  # noqa: ANN001, ARG001
    **kwargs,  # noqa: ANN001, ANN003, ARG001
) -> None:
    return "hsl(0, 0%%, %d%%)" % random.randint(60, 100)  # noqa: S311



def get_posts_from_gcs():
    gcs_path = f'data/rde.parquet'
    gcs_block = GcsBucket.load('zc-gcs')
    gcs_block.get_directory(from_path=gcs_path, local_path='./')
    df = pd.read_parquet(gcs_path)
    return df


def get_comments_from_gcs():
    gcs_path = f'data/cmt_rd.parquet'
    gcs_block = GcsBucket.load('zc-gcs')
    gcs_block.get_directory(from_path=gcs_path, local_path='./')
    df = pd.read_parquet(gcs_path)
    return df

def write_to_gcs(
    local_path: Union[Path, str], gcs_bucket_path: Union[Path, str]) -> None:
    gcs_bucket_block = GcsBucket.load("zc-gcs")
    gcs_bucket_block.upload_from_path(from_path=local_path, to_path=gcs_bucket_path)
    print(f"succesfully uploaded to {gcs_bucket_path}")


def write_local_and_to_gcs(local_path) -> None:
    local_path = local_path
    local_path.parent.mkdir(parents=True, exist_ok=True)
    write_to_gcs(local_path=local_path, gcs_bucket_path=local_path)


@task(log_prints=True)
def create_wordcloud(
    img_file_path: Path, column_name: str, stopwords_list: list[str]
) -> Path:
    if column_name in ["post_title", "post_text"]:
        df = get_posts_from_gcs()
        local_path = Path(f"{img_file_path}/wordcloud_{column_name}.png")
    elif column_name == "body":
        df = get_comments_from_gcs()
        local_path = Path(f"{img_file_path}/wordcloud_{column_name}.png")

    column_to_string: str = "".join(df[column_name].to_list())

    wordcloud = WordCloud(
        colormap="ocean",
        background_color="black",
        mode="RGBA",
        color_func=grey_color_func,
        min_font_size=10,
        stopwords=stopwords_list,
    ).generate(column_to_string)
    # Display the generated image:
    # the matplotlib way:
    plt.imshow(wordcloud, interpolation="bilinear")
    # plt.tight_layout(pad=0)
    plt.axis("off")
    plt.show()
    # plt.savefig(local_path, transparent=True, bbox_inches="tight", pad_inches=0)
    write_local_and_to_gcs(local_path)
    plt.close()


@flow()
def create_plots() -> None:
    matplotlib.use("agg")
    img_file_path = Path("data/img")
    stopwords_personalized = nltk.corpus.stopwords.words("english")
    new_stopwords = [
        "u",
        "/u",
        "would",
        "could",
        "get",
        "got",
        "even",
        "said",
        "wa",
        "around",
        "still",
        "ha",
        "using",
        "best",
        "like",
        "I'm",
        "want",
        "need",
        "use",
        "know",
        "table"
        "one",
        "time",

    ]
    stopwords_personalized.extend(new_stopwords)

    create_wordcloud(
        img_file_path=img_file_path,
        column_name="post_title",
        stopwords_list=stopwords_personalized,
    )
    create_wordcloud(
        img_file_path=img_file_path,
        column_name="post_text",
        stopwords_list=stopwords_personalized,
    )
    create_wordcloud(
        img_file_path=img_file_path,
        column_name="body",
        stopwords_list=stopwords_personalized,
    )


if __name__ == "__main__":
    create_plots()