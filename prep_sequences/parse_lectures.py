from bs4 import BeautifulSoup
import pandas as pd
import sys

lecfile = sys.argv[1]
outhdf5 = sys.argv[2]
lechtml = open(lecfile).read()

soup = BeautifulSoup(lechtml)

lectures = [tag['data-lecture-id'] for tag in soup.findAll('a', attrs={'data-lecture-id': True})]

store = pd.HDFStore(outhdf5)
store['lecture_id'] = pd.Series(lectures).astype(float)
store.close()

print("Wrote lecture order to h5 file")
