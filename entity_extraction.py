from google.cloud import language_v1
from google.cloud.language_v1 import enums
import os
import pickle


# global variables
input_doc_sor_pickle_file = 'target/pickled_doc_sor.pkl'
# ocr_docs_path = '/Users/verlynfischer/enron_entity_data/OcrData' # Local box
ocr_docs_path = '/home/fischer/OcrData' # AI Lab box

# Google connection
# entity_client = language_v1.LanguageServiceClient.from_service_account_json(
#     "/Users/verlynfischer/GoogleProjectKeys/EntityTest-15e051c291e5.json") # Local

entity_client = language_v1.LanguageServiceClient.from_service_account_json(
    "/home/fischer/EntityTest-15e051c291e5.json") # AI Lab

class docStructure:

    def __init__(self):
        self.docID = ''
        self.email_from_name = ''
        self.email_from_address = ''
        self.author = ''
        self.custodian = ''
        self.hash = ''
        self.to_list = []
        self.entity_list = []

class recipientStructure:

    def __init__(self):
        self.email_to_name = ''
        self.email_to_address = ''

class entityStructure:

    def __init__(self):
        self.entityName = ''
        self.entityType = ''
        self.mentionCount = ''
        self.salience = ''
        self.sentimentScore = ''
        self.sentimentMagnitude = ''
        self.meta_list = []

class metaStructure:

    def __init__(self):
        self.metaName = ''
        self.metaValue = ''

def analyzeEntitySentiment(text_content):
    global entity_client

    entity_list = []

    type_ = enums.Document.Type.PLAIN_TEXT
    document = {"content": text_content, "type": type_}
    encoding_type = enums.EncodingType.UTF8

    try:
        response = entity_client.analyze_entity_sentiment(document, encoding_type=encoding_type)

        for entity in response.entities:
            entityObject = entityStructure()
            entityObject.entityName = entity.name
            entityObject.entityType = enums.Entity.Type(entity.type).name
            entityObject.salience = entity.salience
            entityObject.sentimentScore = entity.sentiment.score
            entityObject.sentimentMagnitude = entity.sentiment.magnitude
            entityObject.mentionCount = len(entity.mentions)

            for metadata_name, metadata_value in entity.metadata.items():
                metaObj = metaStructure()
                metaObj.metaName = metadata_name
                metaObj.metaValue = metadata_value
                entityObject.meta_list.append(metaObj)

            entity_list.append(entityObject)
    except:
        pass
    return entity_list

def main():

    global input_doc_sor_pickle_file

    with open(input_doc_sor_pickle_file, 'rb') as f:
        document_list = pickle.load(f)

    print('Pickle File Loaded')

    batch_list = []

    document_index = 0
    batch_index = 0
    sub_doc_index = 0

    for document in document_list:

        document_index += 1
        batch_list.append(document)

        if document_index % 1000 == 0 or document_index == len(document_list):
            batch_index += 1
            for batchDoc in batch_list:
                sub_doc_index += 1
                print(f'Processing Batch: {batch_index}   Batch Doc: {sub_doc_index}')

                sourceTextPath = f'{ocr_docs_path}/{batchDoc.hash[0:4]}/{batchDoc.hash}.txt'

                if os.path.isfile(sourceTextPath):
                    with open(sourceTextPath, 'r') as f:
                        text_content = f.read()
                    batchDoc.entity_list = analyzeEntitySentiment(text_content)

            filePath = f'output/enriched_doc_sor_pickle_file_{batch_index}.pkl'
            with open(filePath,'wb') as f:
                pickle.dump(batch_list,f)
            print(f'Wrote Pickle File for Batch: {batch_index}')
            print()

            batch_list.clear()
            sub_doc_index = 0


    print('Processing Complete')

main()