from langchain_astradb import AstraDBVectorStore
from dotenv import load_dotenv
import pandas
import os

from langchain_google_genai import GoogleGenerativeAIEmbeddings

from data_transform import data_converter

load_dotenv()

ASTRA_DB_API_ENDPOINT=os.getenv("ASTRA_DB_API_ENDPOINT")
ASTRA_DB_APPLICATION_TOKEN=os.getenv("ASTRA_DB_APPLICATION_TOKEN")
ASTRA_DB_KEYSPACE=os.getenv("ASTRA_DB_KEYSPACE")
GOOGLE_API_KEY=os.getenv("GOOGLE_API_KEY")

os.environ["ASTRA_DB_API_ENDPOINT"] = ASTRA_DB_API_ENDPOINT
os.environ["ASTRA_DB_APPLICATION_TOKEN"] = ASTRA_DB_APPLICATION_TOKEN
os.environ["ASTRA_DB_KEYSPACE"] = ASTRA_DB_KEYSPACE
os.environ["GOOGLE_API_KEY"] = GOOGLE_API_KEY

class ingest_data:
    
    def __init__(self):
        """
        data ingest class has been initialized
        """
        print("data ingest class has been initialized")
        self.embeddings = GoogleGenerativeAIEmbeddings(model="models/text-embedding-004")
        self.data_converter = data_converter()

    def data_ingestion1(self, status):
        print("data ingestion method")
        vstore = AstraDBVectorStore(
            embedding=self.embeddings,
            collection_name="chatbotecom",
            api_endpoint=ASTRA_DB_API_ENDPOINT,
            token=ASTRA_DB_APPLICATION_TOKEN,
            namespace=ASTRA_DB_KEYSPACE
        )

        storage = status

        if storage is None:
            docs = self.data_converter.data_transformation()
            inserted_ids = vstore.add_documents(docs)
            print(inserted_ids)
        else:
            return vstore
        
        return vstore, inserted_ids
        

    
if __name__ == "__main__":
    ingest_data = ingest_data()
    vstore, inserted_ids = ingest_data.data_ingestion1(None)
    results = vstore.similarity_search("suggest me a low budget headphone")
    for res in results:
        print(f"{res.page_content} {res.metadata}")

