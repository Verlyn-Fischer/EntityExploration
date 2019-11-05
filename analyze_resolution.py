import csv

author_path = 'er_source/new_er_files/author.csv'
custodian_path = 'er_source/new_er_files/custodian.csv'
email_from_path = 'er_source/new_er_files/email_from.csv'
entity_path = 'er_source/new_er_files/entity.csv'
recipient_path = 'er_source/new_er_files/recipient.csv'
g2export_path = 'er_source/g2export.csv'

author = []
custodian = []
email_from = []
entity = []
recipient = []
g2export = []

class resolvedEntity():

    def __init__(self):
        self.name = ''
        self.ID = ''
        self.mentions = []

class mention():

    def __init__(self):
        self.type = ''
        self.text = ''
        self.address = ''
        self.docID = ''

def importCSV(filePath):

    output = []

    with open(filePath, newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            output.append(row)
            # print(row['RECORD_ID'])
    return output

def getMention(source,record):

    global author
    global custodian
    global email_from
    global entity
    global recipient

    if source == 'AUTHOR':
        objectList = author
    elif source == 'CUSTODIAN':
        objectList = custodian
    elif source == 'EMAIL_FROM':
        objectList = email_from
    elif source == 'ENTITY':
        objectList = entity
    elif source == 'RECIPIENT':
        objectList = recipient

    newMention = None

    for x in objectList:
        if x['RECORD_ID'] == record:
            newMention = mention()
            newMention.type = source
            newMention.text = x['NAME_FULL']
            if 'EMAIL_ADDRESS' in x.keys():
                newMention.address = x['EMAIL_ADDRESS']
            else:
                newMention.address = ''
            newMention.docID = x['docID']
            break

    return newMention

def main():

    global author
    global custodian
    global email_from
    global entity
    global recipient
    global g2export

    author = importCSV(author_path)
    custodian = importCSV(custodian_path)
    email_from = importCSV(email_from_path)
    entity = importCSV(entity_path)
    recipient = importCSV(recipient_path)
    g2export = importCSV(g2export_path)

    resolvedEntities = []

    for entItem in g2export:

        # If entItem already in list then merely add mention
        # If entItem not in list then add entity to list AND add it's mention

        entityInList = False

        for x in resolvedEntities:
            if x.ID == entItem['RESOLVED_ENTITY_ID']:
                entityInList = True
                newMention = getMention(entItem['DATA_SOURCE'], entItem['RECORD_ID'])
                x.mentions.append(newMention)
                break
        if not entityInList:
            newResolvedEntity = resolvedEntity()
            newResolvedEntity.name = 'TBD'
            newResolvedEntity.ID = entItem['RESOLVED_ENTITY_ID']
            newMention = getMention(entItem['DATA_SOURCE'], entItem['RECORD_ID'])
            newResolvedEntity.mentions.append(newMention)
            resolvedEntities.append(newResolvedEntity)

    print('List of Resolved Entities Generated')

    # Print a report

    for x in resolvedEntities:
        print()
        print(f'{x.ID}\t{x.name}')
        for y in x.mentions:
            print(f'\t{y.text}\t{y.type}\t{y.address}\t{y.docID}')


main()