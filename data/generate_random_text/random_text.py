from random import sample
from nltk.corpus import words
import nltk
nltk.download('words')
n = 10000
with open("../random_text.txt", "w+") as file:
    for i in range(300):
        ligne  = " ".join(sample(words.words(), n))
        ligne2  = " ".join(sample(words.words(), n))
        file.write(ligne)