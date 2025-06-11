import pandas as pd
from langchain_core.documents import Document

class data_converter:

    def __init__(self):
        """
        data converter class has been initialized
        """
        print("data converter class has been initialized")
        self.product_details = pd.read_csv(r"D:\Learning Projects\customer_support_system\data\flipkart_product_review.csv")
        # print(self.product_details.head())

    def data_transformation(self):
        required_columns = self.product_details.columns
        required_columns = list(required_columns[1:])
        # print(required_columns)

        # fetch each row into an object and then append it into a product list
        product_list = []
        for index, row in self.product_details.iterrows():

            object = {
                "product_name" : row["product_title"],
                "product_rating" : row["rating"],
                "product_summary" : row["summary"],
                "product_review" : row["review"]
            }
            product_list.append(object)
        # print("*************product list************")
        # print(len(product_list))
        # print(product_list[0])

        # tranform each object from the product list into metadata and Document object and append it into a docs list
        docs = []
        for entry in product_list:
            metadata = {
                "product_name":entry["product_name"],
                "product_rating":entry["product_rating"],
                "product_summary":entry["product_summary"]
                }
            doc = Document(page_content=entry["product_review"], metadata=metadata)
            docs.append(doc)
        
        print(docs[0])
        return docs

if __name__ == "__main__":
    data = data_converter()
    data.data_transformation()
