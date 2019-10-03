import pickle
import requests
import json

updated_doc_sor_entities_file = 'target/updated_doc_sor_entities_1.pkl'

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

class flatDocStructure:

    def __init__(self):
        self.docID = ''
        self.email_from_name = ''
        self.email_from_address = ''
        self.author = ''
        self.custodian = ''
        self.hash = ''
        self.email_to_name = ''
        self.email_to_address = ''
        self.entityName = ''
        self.entityType = ''
        self.mentionCount = ''
        self.salience = ''
        self.sentimentScore = ''
        self.sentimentMagnitude = ''

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

def prepareJSON(document):

    # output_json = 'POST /enron-entities/_doc\n'
    output_json = '{\n'
    output_json = output_json + buildEntry('DocID',document.docID,False)
    output_json = output_json + buildEntry('author',document.author,False)
    output_json = output_json + buildEntry('custodian', document.custodian, False)
    output_json = output_json + buildEntry('hash', document.hash, False)
    output_json = output_json + buildEntry('email-from-displayname', document.email_from_name, False)
    output_json = output_json + buildEntry('email-from-address', document.email_from_address, False)
    output_json = output_json + buildEntry('entity-name', document.entityName, False)
    output_json = output_json + buildEntry('entity-type', document.entityType, False)
    output_json = output_json + buildEntry('mentions', document.mentionCount, False)
    output_json = output_json + buildEntry('salience', document.salience, False)
    output_json = output_json + buildEntry('entity-sentiment-score', document.sentimentScore, False)
    output_json = output_json + buildEntry('entity-sentiment-magnitude', document.sentimentMagnitude, False)
    output_json = output_json + buildEntry('email-to-displayname', document.email_to_name, False)
    output_json = output_json + buildEntry('email-to-address', document.email_to_address, True)
    output_json = output_json + '}\n'

    return output_json

def buildEntry(field,value,terminal):
    if value != None:
        if not terminal:
            return '"' + field + '":"' + str(value) + '",\n'
        else:
            return '"' + field + '":"' + str(value) + '"\n'
    else:
        if not terminal:
            return '"' + field + '":"' + '' + '",\n'
        else:
            return '"' + field + '":"' + '' + '"\n'

def writeToElastic(message):

    headers = {
        'Content-Type': 'application/json',
    }

    response = requests.post('http://10.10.138.98:9200/enron-entities/_doc', headers=headers, data=message)

def main():

    with open(updated_doc_sor_entities_file, 'rb') as f:
        document_list = pickle.load(f)

    document_index = 0
    for document in document_list:
        document_index += 1
        print(f'Processing Document: {document_index}')

        flat_list = []

        if len(document.entity_list) == 0:
            if len(document.to_list) == 0:
                flat = flatDocStructure()
                # Document Attributes
                flat.docID = document.docID
                flat.author = document.author
                flat.custodian = document.custodian
                flat.hash = document.hash
                flat.email_from_name = document.email_from_name
                flat.email_from_address = document.email_from_address
                flat_list.append(flat)
            else:
                for recipient in document.to_list:
                    flat = flatDocStructure()
                    # Document Attributes
                    flat.docID = document.docID
                    flat.author = document.author
                    flat.custodian = document.custodian
                    flat.hash = document.hash
                    flat.email_from_name = document.email_from_name
                    flat.email_from_address = document.email_from_address
                    # Recipient Attributes
                    flat.email_to_name = recipient.email_to_name
                    flat.email_to_address = recipient.email_to_address
                    flat_list.append(flat)
        else:
            if len(document.to_list) == 0:
                for entity in document.entity_list:
                    flat = flatDocStructure()
                    # Document Attributes
                    flat.docID = document.docID
                    flat.author = document.author
                    flat.custodian = document.custodian
                    flat.hash = document.hash
                    flat.email_from_name = document.email_from_name
                    flat.email_from_address = document.email_from_address
                    # Entity Attributes
                    flat.entityName = entity.entityName
                    flat.entityType = entity.entityType
                    flat.mentionCount = entity.mentionCount
                    flat.salience = entity.salience
                    flat.sentimentScore = entity.sentimentScore
                    flat.sentimentMagnitude = entity.sentimentMagnitude
                    flat_list.append(flat)
            else:
                for entity in document.entity_list:
                    for recipient in document.to_list:
                        flat = flatDocStructure()
                        # Document Attributes
                        flat.docID = document.docID
                        flat.author = document.author
                        flat.custodian = document.custodian
                        flat.hash = document.hash
                        flat.email_from_name = document.email_from_name
                        flat.email_from_address = document.email_from_address
                        # Entity Attributes
                        flat.entityName = entity.entityName
                        flat.entityType = entity.entityType
                        flat.mentionCount = entity.mentionCount
                        flat.salience = entity.salience
                        flat.sentimentScore = entity.sentimentScore
                        flat.sentimentMagnitude = entity.sentimentMagnitude
                        # Recipient Attributes
                        flat.email_to_name = recipient.email_to_name
                        flat.email_to_address = recipient.email_to_address
                        flat_list.append(flat)

        for flat in flat_list:
            message = prepareJSON(flat)
            writeToElastic(message)

        if document_index > 1:
            break

main()