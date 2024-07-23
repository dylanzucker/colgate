from elastic_wrapper import ElasticsearchWrapper
if  __name__ == "__main__":
    es_wrapper = ElasticsearchWrapper()
    index='coma_knowledgebase'
    es_wrapper.load_and_index_pdf_documents(common_index_name=index, folder_path="../pdfs")
    #to delete the index, uncomment the line below.
    #es_wrapper.delete_index(index)
   