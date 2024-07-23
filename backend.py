import PyPDF2
import os
from ast import literal_eval
from elastic_wrapper import ElasticsearchWrapper
import json
from ibm_watson_machine_learning.foundation_models import Model
from ibm_watson_machine_learning.metanames import GenTextParamsMetaNames as GenParams
from dotenv import load_dotenv
from prompts import SYSTEM_PROMPT,USER_PROMPT,GRANITEV2_PROMPT,LLAMA3_PROMPT,MIXTRAL_PROMPT

es_wrapper = ElasticsearchWrapper()

DEFAULT_LLM_PARAMS = {
    'decoding_method':"greedy",
    'min_new_tokens':1, 
    'max_new_tokens':400, 
    'repetition_penalty':1,
    'random_seed':42, 
}
DEFAULT_PROMPT='''
You are a helpful, respectful and honest assistant. You will be provided with a context and a query. Use the context to answer the query. Always answer as helpfully as possible, while being safe. Your answers should not include any harmful, unethical, racist, sexist, toxic, dangerous, or illegal content. Please ensure that your responses are socially unbiased and positive in nature. If a question does not make any sense, or is not factually coherent, explain why instead of answering something incorrectly.
If you don't know the answer to a question, please don't share false information. Do not provide any extra information.\n
'''

class Backend():
    def __init__(self,model_id='meta-llama/llama-3-70b-instruct',model_params = DEFAULT_LLM_PARAMS ):
        """
        Initializes the Model instance for a model id and generation parameters
        Returns:
        None
        """

        load_dotenv()
        api_key = os.getenv("IBM_CLOUD_API_KEY", None)        
        ibm_cloud_url = os.getenv("IBM_CLOUD_ENDPOINT", None) 
        project_id = os.getenv("IBM_CLOUD_PROJECT_ID", None)
        
        if api_key is None or ibm_cloud_url is None or project_id is None:
            print("Ensure you copied the .env file into the same directory")
        else:
            self.creds = {
                "url": ibm_cloud_url,
                "apikey": api_key 
            }
        
        self.model_id=model_id
        self.model_params = model_params
        self.model = Model(model_id=self.model_id, params=self.model_params, credentials=self.creds,project_id=project_id)
    
    

    
    def get_relevant_context(self, query):
        context=es_wrapper.clean_output(es_wrapper.hybrid_search(index_name="coma_knowledgebase",text_query=query,top_k=3))
        return context
    
    def generate_response(self, prompt:str = DEFAULT_PROMPT, **kwargs):
        """
        Generate a response using llm based on the provided query and context.
    
        Args:
            query (str): The query for generating the response.
            context (str): The context related to the query.
            prompt_text (str, optional): The prompt used for generating llm output. Defaults to DEFAULT_PROMPT.
            **kwargs: Additional keyword arguments for customization.
    
        Returns:
            str: The generated response.
        """
        result = self.model.generate_text(prompt, **kwargs)
        return result
    
    def generate_stream_response(self, prompt:str = DEFAULT_PROMPT, **kwargs):
        generator = self.model.generate_text_stream(prompt, **kwargs)
        return generator
    
    def build_prompt(self,query,context):
        user_prompt = USER_PROMPT.format(question=query,context=context)
        return LLAMA3_PROMPT.format(system_prompt=SYSTEM_PROMPT,user_prompt=user_prompt)






from example_questions import example_questions
if __name__ == "__main__":
    backend=Backend()
    query="tell me about Cultural Organization Economic Recovery Program Grants??"
    answers = []
    for query in example_questions:
        context=backend.get_relevant_context(query=query)#context for llm from the vector db
        print(context)
        prompt=backend.build_prompt(query,context)
        #print("prompt",prompt)
        result = backend.generate_response(prompt=prompt)#response from llm
        answers.append(result)
    print()
    for query, res in zip(example_questions, answers):    
        print("Question: ", query)
        print()
        print("llm response: ",res)
        print("________")