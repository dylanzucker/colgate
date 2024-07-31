import streamlit as st
import os
from io import BytesIO
from elasticsearch import Elasticsearch
from PyPDF2 import PdfReader
from dotenv import load_dotenv
from sentence_transformers import SentenceTransformer

# Initialize Elasticsearch client
load_dotenv()

es_credentials = {
            "url": os.getenv("ELASTIC_URL",None),
            "username": os.getenv("ELASTIC_USERNAME",None),
            "use_anonymous_access": """false""",
            "password": os.getenv("ELASTIC_PASSWORD",None)
        }
print(es_credentials)
es = Elasticsearch(
            es_credentials["url"],
            basic_auth=(es_credentials["username"], es_credentials["password"]),
            verify_certs=False,
            request_timeout=3600
        )

def text_to_chunks(texts: str, chunk_length: int = 500, chunk_overlap: int = 100) -> list:
        """
        Splits the text into equally distributed chunks with 25-word overlap.
        Args:
            texts (str): Text to be converted into chunks.
            chunk_length (int): Maximum number of words in each chunk.
            chunk_overlap (int): Number of words to overlap between chunks.
        """
        words = texts.split(' ')
        n = len(words)
        chunks = []
        chunk_number = 1
        i = 0
        while i < n:
        # Extract a chunk of words and handle the overlap
            chunk_end = min(i + chunk_length, n)
            chunk = words[i:chunk_end]
            i += chunk_length - chunk_overlap  # Advance the starting index by the length minus the overlap
            chunk_text = ' '.join(chunk).strip()
            chunks.append({"text": chunk_text, "chunk_number": chunk_number})
            chunk_number += 1

        return chunks
import requests
import json
import io
def _hash_content(content: str) -> str:
    return _hash_content_bytes(content.encode())

import hashlib

def _hash_content_bytes(content_bytes: bytes) -> str:
    return hashlib.sha256(content_bytes).hexdigest()

from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk 
import time
def upload_pdf_to_elasticsearch(file, file_name, index_name="colgate_palmolive"):
    # embedding_model_name="sentence-transformers/all-MiniLM-L6-v2"
    # model = SentenceTransformer(embedding_model_name)
    pdf_reader = PdfReader(file)
    index = "search-colgate_elser"
    PIPELINE_NAME = "ml-inference-search-colgate_elser-_elser_model_2_linux-x86_64"#"ml-inference-colgate_palmolive-_elser_model_2_linux-x86_64"
    text = " ".join(page.extract_text() if page.extract_text() else "" for page in pdf_reader.pages)
    chunks = text_to_chunks(texts=text, chunk_length=200, chunk_overlap=25)
    action_chunks = []
    for i, chunk in enumerate(chunks):
        document = {
            "_index": index,
            "index": index,
            "_source": {
                "text": chunk,
                "text_field": chunk,
            },
            "pipeline": PIPELINE_NAME
        }
        action_chunks.append(document)

    for i, chunk in enumerate(chunks):
        url = "https://345a75c1-eb06-4713-abc0-3ed4aff540a2.c5kn1n9d0g7polghe820.databases.appdomain.cloud:30087/colgate_palmolive/_doc?pipeline=colgate_palmolive"
        headers = {
            "Content-Type": "application/json",
            "Authorization": "ApiKey VXk3SUNKRUJ2RU1felZmUFRGNDc6LTVUVWhtZmNTUlNGVDltMkdhaXJVQQ=="
        }

        doc = {
            "pdf_name": file_name.replace(' ', '_').lower(),
            "text": chunk['text'],
            "text_field": chunk['text'],
            "chunk_number": i + 1,
             "_extract_binary_content": True,
            "_reduce_whitespace": True,
            "_run_ml_inference": True,
        }

        res = es.index(index=index, body=doc, pipeline=PIPELINE_NAME)
    return res 

st.title("PDF to Elasticsearch Uploader")
uploaded_files = st.file_uploader("Choose PDF files", accept_multiple_files=True, type="pdf")

if uploaded_files:
    for uploaded_file in uploaded_files:
        # Convert the uploaded file to a BytesIO object
        file_bytes = BytesIO(uploaded_file.read())
        # Upload the file to Elasticsearch
        result = upload_pdf_to_elasticsearch(file_bytes, uploaded_file.name)
        st.write(f"Uploaded {uploaded_file.name} to Elasticsearch. Response: {result}")

