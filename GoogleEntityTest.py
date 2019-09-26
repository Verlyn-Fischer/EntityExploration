from google.cloud import language_v1
from google.cloud.language_v1 import enums
import unicodedata as unicode

source_file_path = "sources/enron_306902.txt"

def sample_analyze_entities(text_content):
    """
    Analyzing Entities in a String

    Args:
      text_content The text content to analyze
    """

    # client = language_v1.LanguageServiceClient()
    client = language_v1.LanguageServiceClient.from_service_account_json("/Users/verlynfischer/GoogleProjectKeys/EntityTest-15e051c291e5.json")

    # text_content = 'California is a state.'

    # Available types: PLAIN_TEXT, HTML
    type_ = enums.Document.Type.PLAIN_TEXT

    # Optional. If not specified, the language is automatically detected.
    # For list of supported languages:
    # https://cloud.google.com/natural-language/docs/languages
    language = "en"
    document = {"content": text_content, "type": type_, "language": language}

    # Available values: NONE, UTF8, UTF16, UTF32
    encoding_type = enums.EncodingType.UTF8

    response = client.analyze_entities(document, encoding_type=encoding_type)
    # Loop through entitites returned from the API
    entitiesFound = []
    for entity in response.entities:
        proper = False
        if enums.Entity.Type(entity.type).name == 'PERSON' or enums.Entity.Type(entity.type).name == 'LOCATION' or enums.Entity.Type(entity.type).name == 'ORGANIZATION':
            for mention in entity.mentions:
                if enums.EntityMention.Type(mention.type).name == 'PROPER':
                    proper = True
            if proper:
                # print(f'Google,{enums.Entity.Type(entity.type).name},{entity.name}')
                entitiesFound.append(entity.name)

    # Deduplicate list of NP within a file
    entitiesFound = list(dict.fromkeys(entitiesFound))

    for item in entitiesFound:
        print(f'Google,{item}')

            # print(u"Representative name for the entity: {}".format(entity.name))
            # # Get entity type, e.g. PERSON, LOCATION, ADDRESS, NUMBER, et al
            # print(u"Entity type: {}".format(enums.Entity.Type(entity.type).name))
            # # Get the salience score associated with the entity in the [0, 1.0] range
            # print(u"Salience score: {}".format(entity.salience))
            # print()
            # Loop over the metadata associated with entity. For many known entities,
            # the metadata is a Wikipedia URL (wikipedia_url) and Knowledge Graph MID (mid).
            # Some entity types may have additional metadata, e.g. ADDRESS entities
            # may have metadata for the address street_name, postal_code, et al.
        #     for metadata_name, metadata_value in entity.metadata.items():
        #         print(u"{}: {}".format(metadata_name, metadata_value))
        #
        #     # Loop over the mentions of this entity in the input document.
        #     # The API currently supports proper noun mentions.
        #     for mention in entity.mentions:
        #         print(u"Mention text: {}".format(mention.text.content))
        #         # Get the mention type, e.g. PROPER for proper noun
        #         print(
        #             u"Mention type: {}".format(enums.EntityMention.Type(mention.type).name)
        #         )
        #
        # # Get the language of the text, which will be the same as
        # # the language specified in the request or, if not specified,
        # # the automatically-detected language.
        # print(u"Language of the text: {}".format(response.language))

def main():
    sourceFile = open(source_file_path,"r")

    with open(source_file_path, 'r') as file:
        # text_content = file.read().replace('\n', '')
        text_content = file.read()


    # text_content = "Michelangelo Caravaggio, Italian painter, is known for 'The Calling of Saint Matthew'."
    sample_analyze_entities(text_content)

main()