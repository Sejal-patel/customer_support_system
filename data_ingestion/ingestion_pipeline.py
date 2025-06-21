import os
import pandas as pd
from utils.model_loader import ModelLoader
from config.config_loader import load_config
from dotenv import load_dotenv
from langchain.schema import Document
from  typing import List
from langchain_astradb import AstraDBVectorStore

class DataIngestion:

    def __init__(self):
        self.model_loader = ModelLoader()
        self.load_env_vars()
        self.file_path = self.get_file_path()
        self.data = self.load_csv_data()
        self.config = load_config()


    # load model
    def load_model(self):
        self.model_loader.load_embedding_model()


    # load env variables
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

    # get the data file path
    def get_file_path(self):
        """
        Get the csv file path located in 'data' folder
        """
        current_dir = os.getcwd()
        csv_path = os.path.join(current_dir, 'data', 'flipkart_product_review.csv')
        
        if not os.path.exists(csv_path):
            raise FileNotFoundError(f"CSV file not found at {csv_path}")
        
        return csv_path


    # load the data
    def load_csv_data(self):
        """
        Load the CSV data from the specified file path.
        """
        expected_columns = ['product_id','product_title','rating','summary','review']

        data = pd.read_csv(self.file_path)

        if data.empty:
            raise ValueError("CSV file is empty")
        return data
        

    # transform data
    def transform_data(self, data):
        """
        Transform the data to the required format.
        """
        
        documents = []
        for index, row in data.iterrows():
            metadata = {
                'product_id':row['product_id'],
                'product_title': row['product_title'],
                'rating': row['rating'],
                'summary': row['summary']
            }
            document = Document(
                page_content = row['review'],
                metadata = metadata
            )
            documents.append(document)

        return documents
            

    # create embeddings from the data
    def create_embeddings(self, documents):
        """
        Create embeddings for the documents.
        """
        # Load the embedding model
        embedding_model = self.load_model()
        
        # Generate embeddings
        embeddings = embedding_model.embed_documents(documents)
        
        return embeddings

    # store the embedding into vectore store
    def store_in_vector_db(self, documents: List[Document]):
        """
        Store the embeddings into the vector store.
        """
        vstore = AstraDBVectorStore(
            api_endpoint=self.astra_db_api_endpoint,
            token=self.astra_db_application_token,
            namespace=self.astra_db_keyspace,
            collection_name=self.config['astra_db']['collection_name'],
            embedding=self.model_loader.load_embedding_model()
        )
        inserted_ids = vstore.add_documents(documents)
        if not inserted_ids:
            raise ValueError("No documents were inserted into the vector store.")
        print(f"Inserted {len(inserted_ids)} documents into the vector store.")
        return vstore, inserted_ids

    def run_pipeline(self):
        """
        Run the data ingestion pipeline.
        """
        
        # Transform data
        documents = self.transform_data(self.data)
        
        # Store in vector store
        vstore, inserted_ids = self.store_in_vector_db(documents)

        # query the vector store
        query = "What is the rating of the product?"
        results = vstore.similarity_search(query, k=self.config['retriever']['top_k'])

        for result in results:
            print(f"Content: {result.page_content}\nMetadata: {result.metadata}")
            # print(result.page_content)
            # print(f"Product ID: {result.metadata['product_id']}, Rating: {result.metadata['rating']}, Summary: {result.metadata['summary']}, 'Review': {result.page_content}")
        
        print(f"Data ingestion completed. {len(inserted_ids)} documents inserted into the vector store.")

if __name__ == "__main__":
    ingest_data = DataIngestion()
    ingest_data.run_pipeline()
    print("Data ingestion pipeline completed successfully.")
