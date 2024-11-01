import spacy

# Load the spacy model that you have installed
nlp = spacy.load('en_core_web_lg')

# process a sentence using the model
doc = nlp("Thanos")

print(doc)
# It's that simple - all of the vectors and words are assigned after this point
# Get the vector for 'text':
print(doc[0].vector.tolist())

# Get the mean vector for the entire sentence (useful for sentence classification etc.)
#print(doc.vector)