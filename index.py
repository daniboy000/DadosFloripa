import urllib2
from bs4 import BeautifulSoup

# imobUrl = 'http://www.maristelaimoveis.com.br'
imobUrl = 'http://www.imobiliariadanielcarvalho.com.br/'

response = urllib2.urlopen(imobUrl)
print response.info()

html = response.read()
# print html

soup = BeautifulSoup(html, 'html.parser')
print soup.title

# get all links
links = soup.find_all('a')
for link in links:
    print link.get('href').encode('utf-8')
    print link.contents
