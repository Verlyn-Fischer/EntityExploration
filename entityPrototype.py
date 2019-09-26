from google.cloud import language_v1
from google.cloud.language_v1 import enums
import mysql.connector as conn

# Connections
s3_source = ''
cnx = []
google_client = language_v1.LanguageServiceClient.from_service_account_json(
    "/Users/verlynfischer/GoogleProjectKeys/EntityTest-15e051c291e5.json")
elastic_client = ''

# global variables

document_list = []

# data structures

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


#####################################################################################################################
#
# 1.0 Query SOR for all records where text is more than 10 characters and where DeDupId has only a single '/'
# 1.1 For each record above, download text file from S3
# 2 Send request to Google
# 3 Receive response from Google
# 4 Construct elastic Entry and send to Elastic
#
#####################################################################################################################

#### Parsing Functions ###
##########################

def getFromName(emailField):
    return ''

def getFromAddress(emailField):
    return ''

def buildToList(emailField):
    return ''

#### MySQL Calls ###
####################

def establishConnection():

    global cnx

    cnx = conn.connect(user='discotest', password='discotest',
                              host='ss-disco-docs-sor-rds-01-test-us-west-2-default.cluster-cwmivtlouy5i.us-west-2.rds.amazonaws.com',
                              database='disco_1ad3e6aa59f248ab8f8e466983b3de31')

def closeConnection():

    cnx.close()

def readFromDocInstances():

    global document_list
    global cnx

    establishConnection()

    query = ("SELECT di.TextLength,di.DeDupId,di.EmailParticipants,di.Author,di.Custodian,di.Hash\
    FROM Docs AS d\
    INNER JOIN DocInstances AS di\
    	ON d.Id = di.DeDupId\
    INNER JOIN\
    (\
    SELECT DeDupId, MAX(SortID) maxSortID\
    FROM DocInstances\
    GROUP BY DeDupId\
    )\
    b ON di.DeDupId = b.DeDupId AND\
    di.SortID = b.maxSortID;")

    cursor = cnx.cursor(buffered=True)

    cursor.execute(query)

    for document in cursor:
        documentObject = docStructure()

        documentObject.TextLength = document[0]
        documentObject.docID = document[1]
        documentObject.email_from_name = getFromName(document[2])
        documentObject.emal_from_address = getFromAddress(document[2])
        documentObject.to_list = buildToList(document[2])
        documentObject.author = document[3]
        documentObject.custodian = document[4]
        documentObject.hash = document[5]

        document_list.append(documentObject)

    cursor.close()
    closeConnection()

#### Google Calls ####
######################

def analyzeEntities(text_content):
    global google_client

    entity_list = []

    type_ = enums.Document.Type.PLAIN_TEXT
    language = "en"
    document = {"content": text_content, "type": type_, "language": language}
    encoding_type = enums.EncodingType.UTF8
    response = google_client.analyze_entity_sentiment(document, encoding_type=encoding_type)

    for entity in response.entities:
        entityObject = entityStructure()
        entityObject.entityName = entity.name
        entityObject.entityType = enums.Entity.Type(entity.type).name
        entityObject.salience = entity.salience
        entityObject.sentimentScore = entity.sentiment.score
        entityObject.sentimentMagnitude = entity.sentiment.magnitude
        entityObject.mentionCount = len(entity.mentions)
        entity_list.append(entityObject)

    return entity_list

#### Elastic Preparation ####
#############################

def prepareJSON(document):

    output_json = 'POST /enron-entities/_doc\n'
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

    return output_json

def buildEntry(field,value,terminal):

    if terminal:
        return '"' + field + '":"' + value + '",\n'
    else:
        return '"' + field + '":"' + value + '"\n'

def main():

    global document_list

    # Query DocSOR
    readFromDocInstances()

    document_index = 0
    for document in document_list:

        document_index += 1
        print(f'Document Processing: {document_index}')

        # Get file from S3
        text_content = 'foo'

        # Hit Google with content
        document.entity_list = analyzeEntities(text_content)

        # Add sentiment into documents

        # Prepare for write to Elastic
        # Mux across documents to prepare messages!!!
        flatDocument = flatDocStructure()
        message = prepareJSON(flatDocument)

        # Write to Elastic



main()

############################################
# SCRAP
#     with open('sources/enron_306902.txt',"r") as file:
#         text_content = file.read()
#
#     print(analyzeEntities(text_content))


