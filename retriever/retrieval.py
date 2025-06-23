import os
from dotenv import load_dotenv
from utils.model_loader import ModelLoader
from config.config_loader import load_config
from langchain_astradb import AstraDBVectorStore


class Retriever:
    def __init__(self):
        self.load_env_vars()
        self.model_loader = ModelLoader()
        self.config = load_config()
        self.vstore = None
        self.retriever = None

    def load_env_vars(self):
        """
        Load and validate required environment variables.
        """
        load_dotenv()

        required_vars = ["ASTRA_DB_API_ENDPOINT","ASTRA_DB_APPLICATION_TOKEN","ASTRA_DB_KEYSPACE","GOOGLE_API_KEY"]

        missing_vars = []
        missing_vars = [var for var in required_vars if not os.getenv(var)]
        if missing_vars:
            raise EnvironmentError(f"Missing variables : {missing_vars}")
        
        self.astra_db_api_endpoint = os.getenv("ASTRA_DB_API_ENDPOINT")
        self.astra_db_application_token = os.getenv("ASTRA_DB_APPLICATION_TOKEN")
        self.astra_db_keyspace = os.getenv("ASTRA_DB_KEYSPACE")
        self.google_api_key = os.getenv("GOOGLE_API_KEY")

    def load_retriever(self):
        """
        Load the retriever using the vector store.
        """

        if not self.vstore:
            collection_name = self.config['astra_db']['collection_name']

            self.vstore = AstraDBVectorStore(
            api_endpoint=self.astra_db_api_endpoint,
            token=self.astra_db_application_token,
            namespace=self.astra_db_keyspace,
            collection_name=collection_name,
            embedding=self.model_loader.load_embedding_model()
        )

        if not self.retriever:
            retriever = self.vstore.as_retriever(search_kwargs = {'top_k': self.config['retriever']['top_k']})
            return retriever
        
    def call_retriever(self, query):
        """
        Call the retriever with a query.
        """
        
        retriever = self.load_retriever()
        output = retriever.invoke(query)
        return output
    
if __name__ == "__main__":
    retriever = Retriever()
    query = "What is the top 2 products in the category of 'Electronics'?"
    results = retriever.call_retriever(query)
    for idx, doc in enumerate(results):
        print(f"Query: {query} \nResult: {doc.page_content} \nMetadata: {doc.metadata}\n")
    print("Retrieval complete.")

    
