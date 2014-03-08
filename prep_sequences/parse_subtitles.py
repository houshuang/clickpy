from bs4 import BeautifulSoup
import pandas as pd

lechtml = open("mentalhealth_002_lectures.html").read()

soup = BeautifulSoup(lechtml)

lectures = [tag['href'] for tag in soup.findAll('a', attrs={'title': "Subtitles (text)"})]
print(lectures)

# download all the text files, and dump them into hdf5.
exit()
store = pd.HDFStore("mentalhealth_002.h5.repacked")


store['lecture_id'] = pd.Series(lectures).astype(float)
store.close()

print("Wrote lecture order to h5 file")
