import mysql.connector as conn
import json_tricks
import pickle

# global variables
document_list = []
cnx = []
outputFile = 'target/pickled_doc_sor.pkl'

#### Data Structures ####

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

#### Parsing Functions ###

def processEmail(document,emailParticipants):
    # parts = '[{"DisplayName": "Lands\' End Shipping", "EmailAddress": "customerrequest@lesn.rsc02.com", "Role": 1}, {"DisplayName": "brapp@enron.com", "EmailAddress": "brapp@enron.com", "Role": 2}]'
    parts_list = json_tricks.loads(emailParticipants)
    for item in parts_list:
        foundToList = False
        if "Role" in item:
            if item["Role"] == 1:
                if "DisplayName" in item:
                    document.email_from_name = item["DisplayName"]
                if "EmailAddress" in item:
                    document.email_from_address = item["EmailAddress"]
            elif item["Role"] == 2 or item["Role"] == 3:
                toItem = recipientStructure()
                if "DisplayName" in item:
                    toItem.email_to_name = item["DisplayName"]
                    foundToList = True
                if "EmailAddress" in item:
                    toItem.email_to_address = item["EmailAddress"]
                    foundToList = True
                if foundToList:
                    document.to_list.append(toItem)

#### MySQL Calls ###

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

    query = ("SELECT di.TextLength,di.DeDupId,di.EmailParticipants,di.Author,di.Custodian,di.DeDupHash\
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
        processEmail(documentObject,document[2])
        documentObject.author = document[3]
        documentObject.custodian = document[4]
        documentObject.hash = document[5]

        document_list.append(documentObject)

    cursor.close()
    closeConnection()

#### Main ####

def main():
    global outputFile
    global document_list

    readFromDocInstances()
    with open(outputFile, 'wb') as f:
        pickle.dump(document_list, f)

    print('Pickle Complete.')

main()