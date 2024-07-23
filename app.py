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

def text_to_chunks(texts: str, chunk_length: int = 150, chunk_overlap: int = 25) -> list:
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

def upload_pdf_to_elasticsearch(file, file_name, index_name="colgate_palmolive"):
    embedding_model_name="sentence-transformers/all-MiniLM-L6-v2"
    model = SentenceTransformer(embedding_model_name)
    pdf_reader = PdfReader(file)
    text = " ".join(page.extract_text() if page.extract_text() else "" for page in pdf_reader.pages)
    chunks = text_to_chunks(texts=text, chunk_length=200, chunk_overlap=25)
    for i, chunk in enumerate(chunks):
        embedding = model.encode(chunk['text'], convert_to_tensor=True).tolist()  # Convert tensor to list directly
        doc = {
            "pdf_name": file_name.replace(' ', '_').lower(),
            "text": chunk['text'],
            "chunk_number": i + 1,
            "embedding": embedding
        }
        res = es.index(index=index_name, body=doc)
        
        print(f"Added chunk {i} of {file_name}")
    # # Create a document with the extracted text and some metadata
    # doc = {
    #     "file_name": file.name,
    #     "content": text,
    #     "page_count": pdf_reader.getNumPages()
    # }
    
    # # Upload the document to Elasticsearch
    # res = es.index(index=index_name, body=doc)
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


