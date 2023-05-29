from bs4 import BeautifulSoup

with open('oneplus.html', 'r') as f:
    content = f.read()

soup = BeautifulSoup(content, 'html.parser')
parent = soup.find_all(attrs={'data-hook': 'review'}, class_='a-section review aok-relative')

ratings = []
username = []
review = []
comment = []
helpful = []
date = []
for i in range(len(parent)):
    print(i)
    username.append(parent[i].find('span', class_='a-profile-name').get_text())

    ratings.append(parent[i].find('span', class_='a-icon-alt').get_text())

    comment.append(parent[i].find(attrs={'data-hook', 'review-title'}).contents[1].get_text())

    date.append(parent[i].find(attrs={'data-hook': 'review-date'}).get_text())

    review.append(parent[i].find(attrs={'data-hook': 'review-body'}).contents[1].get_text())

    var = parent[i].find(attrs={'data-hook': 'helpful-vote-statement'})
    if var:
        helpful.append(var.get_text())
    else:
        helpful.append(None)




import pandas as pd
from itertools import zip_longest

data = zip_longest(username, ratings, comment, helpful, date, review, fillvalue=None)

df = pd.DataFrame(data, columns=['username', 'ratings', 'comment', 'helpful', 'date', 'review'])

df.to_csv('oneplus.csv')

