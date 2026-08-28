import pytest
import os
import time
from neo4j import GraphDatabase
from redis import Redis

@pytest.fixture(scope="session")
def setup_services():
    neo4j_driver = GraphDatabase.driver(
        "bolt://localhost:7687",
        auth=("neo4j", "password")
    )
    redis_client = Redis(host="localhost", port=6379)
    
    yield
    
    neo4j_driver.close()
    redis_client.close()

@pytest.fixture
def clean_neo4j():
    driver = GraphDatabase.driver(
        "bolt://localhost:7687",
        auth=("neo4j", "password")
    )
    with driver.session() as session:
        session.run("MATCH (n) DETACH DELETE n")
    yield driver
    driver.close()

@pytest.fixture
def clean_redis():
    client = Redis(host="localhost", port=6379, decode_responses=True)
    client.flushall()
    yield client
    client.flushall()