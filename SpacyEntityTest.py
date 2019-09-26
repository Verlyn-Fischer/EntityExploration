import spacy
import itertools

# This extracts noun phrases from all of City of Houston matter and places results into a file
# format this can be imported into ElasticSearch

source_file_path = "sources/enron_306902.txt"

def main():
    nlp = spacy.load('en_core_web_lg')
    f = open(source_file_path, "r")
    contents = f.read()
    if len(contents) > 1000000:
        contents = contents[:999998]
    doc = nlp(contents)
    local_chunkList = []
    for chunk in doc.ents:
        if chunk.label_ == 'PERSON' or chunk.label_ == 'ORG' or chunk.label_ == 'GPE':

            keep = True

            firstToken = chunk[0].text

            while nlp.vocab[firstToken].is_stop and len(chunk) > 1:
                chunk = chunk[1:len(chunk)]
                firstToken = chunk[0].text

            lastToken = chunk[len(chunk) - 1].text

            while nlp.vocab[lastToken].is_stop and len(chunk) > 1:
                chunk = chunk[0:len(chunk) - 1]
                lastToken = chunk[len(chunk) - 1].text

            if len(chunk) == 0:
                keep = False

            # for token in chunk:
            #     if not token.is_alpha:
            #         keep = False

            # if chunk.text.isupper():
            #     keep = False

            if keep:
                name = chunk.text.replace('\n', ' ')
                local_chunkList.append([name,chunk.sentiment])

    # Deduplicate list of NP within a file
    # local_chunkList = list(dict.fromkeys(local_chunkList))

    # Print to output
    for item in local_chunkList:
        print(f'Spacy,{item[0]},{item[1]}')
    print(f'Sentiment Score: {doc.sentiment}')


main()