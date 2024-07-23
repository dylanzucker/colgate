from elastic_wrapper import ElasticsearchWrapper
import json
import os
import shutil

def move_pdfs_if_absent( src_folder, target_folder, common_index_name):

        # List all files in the source ('docs') and target ('pdfs') folders
        src_files = {file for file in os.listdir(src_folder) if file.endswith('.pdf')}
        target_files = {file for file in os.listdir(target_folder) if file.endswith('.pdf')}

        # Iterate through source files and move them to the target if they aren't there already
        for file in src_files:
            if file not in target_files:
                es_wrapper.load_and_index_uploaded_documents(common_index_name, src_folder)
                src_path = os.path.join(src_folder, file)
                target_path = os.path.join(target_folder, file)
                shutil.move(src_path, target_path)
                print(f"Moved '{file}' from '{src_folder}' to '{target_folder}'.")
            else:
                print(f"'{file}' already exists in '{target_folder}', not moved.")
                
if __name__ == "__main__":
    es_wrapper=ElasticsearchWrapper()
    move_pdfs_if_absent(src_folder="../docs", target_folder="../pdfs", common_index_name='coma_knowledgebase')
    #es_wrapper.delete_index(index)