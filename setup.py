from setuptools import find_packages, setup

setup(
    name="dagster_poc",
    packages=find_packages(exclude=["dagster_poc_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud",
        "boto3",
        "pandas",
        "matplotlib",
        "textblob",
        "tweepy",
        "wordcloud",
    ],
    extras_require={"dev": ["dagit", "pytest"]},
)
