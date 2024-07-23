import json
from elasticsearch import Elasticsearch,helpers
from sentence_transformers import SentenceTransformer
import numpy as np
from dotenv import load_dotenv
import os
import PyPDF2
import uuid

class ElasticsearchWrapper:
    def __init__(self, embedding_model_name="sentence-transformers/all-MiniLM-L6-v2"):
        load_dotenv()
        self.es_credentials = {
            "url": os.getenv("ELASTIC_URL",None),
            "username": os.getenv("ELASTIC_USERNAME",None),
            "use_anonymous_access": """false""",
            "password": os.getenv("ELASTIC_PASSWORD",None)
        }
        print(self.es_credentials)
        self.client = Elasticsearch(
            self.es_credentials["url"],
            basic_auth=(self.es_credentials["username"], self.es_credentials["password"]),
            verify_certs=False,
            request_timeout=3600
        )
        self.model = SentenceTransformer(embedding_model_name)

    def create_index(self, index_name):
        if self.client.indices.exists(index=index_name):
            self.client.indices.delete(index=index_name)
        index_body = {
            "mappings": {
                "properties": {
                    "pdf_name":{"type": "text"},
                    "text": {"type": "text"},
                    "chunk_number": {"type": "integer"},
                    "embedding": {"type": "dense_vector", "dims": 384},
                }
            }
        }
        self.client.indices.create(index=index_name, body=index_body)

    def delete_index(self, index_name):
        if self.client.indices.exists(index=index_name):
            self.client.indices.delete(index=index_name)
    
    def add_document(self, index_name, doc):
        self.client.index(index=index_name, body=doc)
    
    
    def ingest_bulk(self,index,documents):
        index_documents = [{"_index":index, "_source":source} for source in documents]
        helpers.bulk(self.client,index_documents)



    def text_to_chunks(self,texts: str, chunk_length: int = 150, chunk_overlap: int = 25) -> list:
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

            

    def load_and_index_uploaded_documents(self, common_index_name,folder_path='../docs', ):
    # List all PDF files in the docs folder
        pdf_files = [f for f in os.listdir(folder_path) if f.endswith('.pdf')]
        if not pdf_files:
            print("No PDF files found in the folder.")
            return

        for pdf_file in pdf_files:
            file_path = os.path.join(folder_path, pdf_file)
            
            # Open the PDF file
            with open(file_path, "rb") as file:
                pdf_reader = PyPDF2.PdfReader(file)
                full_text = " ".join(page.extract_text() if page.extract_text() else "" for page in pdf_reader.pages)
                
                chunks = self.text_to_chunks(texts=full_text, chunk_length=200, chunk_overlap=25)
                for i, chunk in enumerate(chunks):
                    embedding = self.model.encode(chunk['text'], convert_to_tensor=True).tolist()  # Convert tensor to list directly
                    doc = {
                        "pdf_name": pdf_file.replace('.pdf', '').replace(' ', '_').lower(),  # Consistent naming for PDF files
                        "text": chunk['text'],
                        "chunk_number": i + 1,
                        "embedding": embedding
                    }
                    self.add_document(index_name=common_index_name, doc=doc)
                    print(f"Added chunk {i} of {pdf_file}")

        print(f"All documents added to the index: {common_index_name}")



    def load_and_index_pdf_documents(self, common_index_name, folder_path='../pdfs'):
        # List all PDF files in the pdfs folder
            pdf_files = [f for f in os.listdir(folder_path) if f.endswith('.pdf')]
            if not pdf_files:
                print("No PDF files found in the folder.")
                return

            # Create a common index for all documents
            self.create_index(common_index_name)

            for pdf_file in pdf_files:
                file_path = os.path.join(folder_path, pdf_file)
                
                # Open the PDF file
                with open(file_path, "rb") as file:
                    pdf_reader = PyPDF2.PdfReader(file)
                    full_text = " ".join(page.extract_text() if page.extract_text() else "" for page in pdf_reader.pages)
                    
                    chunks = self.text_to_chunks(texts=full_text, chunk_length=200, chunk_overlap=25)
                    for i, chunk in enumerate(chunks):
                        embedding = self.model.encode(chunk['text'], convert_to_tensor=True).tolist()  # Convert tensor to list directly
                        doc = {
                            "pdf_name": pdf_file.replace(' ', '_').lower(),
                            "text": chunk['text'],
                            "chunk_number": i + 1,
                            "embedding": embedding
                        }
                        self.add_document(index_name=common_index_name, doc=doc)
                        print(f"Added chunk {i} of {pdf_file}")

            print(f"All documents added to the index: {common_index_name}")
     
                
    def search_by_keyword(self, index_name, keyword):
        query = {
            "query": {
                "match": {
                    "text": keyword
                }
            }
        }
        return self.client.search(index=index_name, body=query)

    def search_by_vector(self, index_name, query_text, top_k=3):
        query_vector = self.model.encode(query_text)#.numpy()
        query = {
            "size": top_k,
            "query": {
                "script_score": {
                    "query": {"match_all": {}},
                    "script": {
                        "source": "cosineSimilarity(params.query_vector, 'embedding') + 1.0",
                        "params": {"query_vector": query_vector}
                    }
                }
            }
        }
        return self.client.search(index=index_name, body=query)

    def clean_output(self, elastic_response):
        print(type(elastic_response))
        hits = elastic_response['hits']['hits']

        # Create a set to keep track of unique paragraphs
        seen_paragraphs = set()

        cleaned_response = []
        for hit in hits:
            paragraph = hit["_source"]['text']
            paragraph = " ".join(paragraph.split())  # Normalize whitespace
            pdf_name = hit["_source"]['pdf_name']  # Extract pdf_name

            if paragraph not in seen_paragraphs:
                seen_paragraphs.add(paragraph)
                cleaned_response.append({
                    'pdf_name': pdf_name,
                    'text': paragraph
                })
            else:
                print("This paragraph was seen, so removing it: ", paragraph)

        return cleaned_response

    
    def hybrid_search(self, index_name, text_query, top_k=3):
        query_vector = self.model.encode(text_query)#.numpy()
        query = {
            "size": top_k,
            "query": {
                "bool": {
                    "must": {
                        "match": {
                            "text": text_query
                        }
                    },
                    "should": {
                        "script_score": {
                            "query": {"match_all": {}},
                            "script": {
                                "source": "cosineSimilarity(params.query_vector, 'embedding') + 1.0",
                                "params": {"query_vector": query_vector}
                            }
                        }
                    }
                }
            }
        }
        return self.client.search(index=index_name, body=query)



