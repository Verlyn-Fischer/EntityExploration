import sqlite3
import os
import pickle
import string

source_path = '/Users/verlynfischer/enron_entity_data/enriched_pickles/output'
database = 'enron_db'
conn = []

# Need to handle apostophes - DONE
# Need to handle control characters in the input - DONE
# Need to enter entities - DONE
# Need to enter recipients
# Need to check for whether entities are acceptable


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

def isQualityEntity(entityName):

    # Must have some lowercase
    # Must have at least a single capital
    # Must be no longer than 28 characters
    # Must be longer than three characters
    # Must contain no more than three punctuation marks and no more than 4 numbers
    # Must not start with http:// or https://
    # Must not contain from to sent subject date

    quality = True

    if entityName.isupper():
        quality = False
    if entityName.islower():
        quality = False
    if len(entityName) > 28:
        quality = False
    if len(entityName) < 4:
        quality = False

    count = 0
    for i in range(len(entityName)):
        char = entityName[i]
        # Checks whether given character is a punctuation mark
        if char in ['!', ",", "\'", ";", "\"", ".", "-", "?"]:
            count += 1
    if count > 3:
        quality = False

    count = 0
    for i in range(len(entityName)):
        char = entityName[i]
        # Checks whether given character is a punctuation mark
        if char in ['1','2','3','4','5','6','7','8','9','0']:
            count += 1
    if count > 4:
        quality = False

    if entityName.find('http://') != -1:
        quality = False

    if entityName.find('From') != -1:
        quality = False

    if entityName.find('To') != -1:
        quality = False

    if entityName.find('Sent') != -1:
        quality = False

    if entityName.find('Subject') != -1:
        quality = False

    if entityName.find('Date') != -1:
        quality = False

    return quality

def makeDocString(doc):
    output = ''

    if doc.docID is None:
        output = output + '("' + '' + '",'
    else:
        output = output + '("' + doc.docID + '",'

    if doc.author is None:
        output = output + '"' + '' + '",'
    else:
        auth = doc.author.replace('"', '')
        output = output + '"' + auth + '",'

    if doc.custodian is None:
        output = output + '"' + '' + '",'
    else:
        cust = doc.custodian.replace('"', '')
        output = output + '"' + cust + '",'

    if doc.email_from_address is None:
        output = output + '"' + '' + '",'
    else:
        email_a = doc.email_from_address.replace('"', '')
        output = output + '"' + email_a + '",'

    if doc.email_from_name is None:
        output = output + '"' + '' + '",'
    else:
        email_n = doc.email_from_name.replace('"', '')
        output = output + '"' + email_n + '",'

    if doc.hash is None:
        output = output + '"' + '' + '",'
    else:
        output = output + '"' + doc.hash + '",'

    if doc.TextLength is None:
        output = output + '"' + '' + '")'
    else:
        output = output + '"' + str(doc.TextLength) + '")'

    return output

def makeEntString(ent,docID):
    output = ''

    if ent.entityName is None:
        output = output + '("' + '' + '",'
    else:
        # output = output + '("' + ent.entityName.encode('utf-8') + '",'
        output = output + '("' + ent.entityName + '",'

    if ent.entityType is None:
        output = output + '"' + '' + '",'
    else:
        output = output + '"' + ent.entityType + '",'

    if ent.mentionCount is None:
        output = output + '"' + '' + '",'
    else:
        output = output + '"' + str(ent.mentionCount) + '",'

    if ent.salience is None:
        output = output + '"' + '' + '",'
    else:
        output = output + '"' + str(ent.salience) + '",'

    if ent.sentimentMagnitude is None:
        output = output + '"' + '' + '",'
    else:
        output = output + '"' + str(ent.sentimentMagnitude) + '",'

    if ent.sentimentScore is None:
        output = output + '"' + '' + '",'
    else:
        output = output + '"' + str(ent.sentimentScore) + '",'

    output = output + '"' + docID + '")'

    return output

def makeRecString(recipient,docID):
    output = ''

    if recipient.email_to_name is None:
        output = output + '("' + '' + '",'
    else:
        email_n = recipient.email_to_name.replace('"','')
        output = output + '("' + email_n + '",'

    if recipient.email_to_address is None:
        output = output + '"' + '' + '",'
    else:
        email_a = recipient.email_to_address.replace('"', '')
        output = output + '"' + email_a + '",'

    output = output + '"' + docID + '")'

    return output

def insertToDB():

    global source_path
    global conn

    fileIndex = 0
    docIndex = 0

    conn = sqlite3.connect(database)
    c = conn.cursor()

    for dirpath, dirnames, filenames in os.walk(source_path):
        for file in filenames:
            if file != '.DS_Store':
                fileIndex += 1
                if fileIndex > 400 and fileIndex < 500:
                    filePath = os.path.join(dirpath, file)
                    with open(filePath, 'rb') as f:
                        document_list = pickle.load(f)

                    for doc in document_list:
                        docIndex += 1
                        docID = doc.docID
                        if docIndex >= 405000 and docIndex < 407000:
                            insertionString = 'INSERT INTO documents VALUES ' + makeDocString(doc)
                            # print(insertionString)
                            c.execute(f'{insertionString}')
                            for ent in doc.entity_list:
                                if isQualityEntity(ent.entityName):
                                    insertionString = 'INSERT INTO entities VALUES ' + makeEntString(ent, docID)
                                    c.execute(f'{insertionString}')
                            for recipient in doc.to_list:
                                insertionString = 'INSERT INTO email_to VALUES ' + makeRecString(recipient, docID)
                                c.execute(f'{insertionString}')
                    print(f'File Num: {fileIndex}  Path: {file}')
                    conn.commit()
    conn.close()

def main():
    insertToDB()

main()
