# -*- coding: utf-8 -*-

import urllib2
from bs4 import BeautifulSoup
import db_manager as dbm

class PageContent:
    url = ""
    rawContent = ""
    soupContent = None

    def __init__(self, url):
        self.url = url
        self.setPageContent()
        self.createSoupContent()

    def setPageContent(self):
        self.rawContent = urllib2.urlopen(self.url).read()

    def getPageContent(self):
        return self.rawContent

    def createSoupContent(self):
        self.soupContent = BeautifulSoup(self.rawContent, 'html.parser')

    def getPageTitle(self):
        return self.soupContent.title

    def getTables(self, print_tables=False):
        tables = self.soupContent.find_all('table')

        print "num tables: ", len(tables)

        # for table in tables:
        #     print "sub tables: ", len(table.find_all('table'))

        if (print_tables):
            for i in range(len(tables)):
                print 'TABLE %i' % i
                print tables[i]
                print
                print

        return tables

    def getSubTables(self):
        tables = self.getTables()
        for table in tables:
            sub_tables = table.find_all('table')
            if len(sub_tables) > 0:
                return sub_tables

    def getLinks(self):
        links = self.soupContent.find_all('a')
        return links

if __name__ == "__main__":
    mainURL = 'http://www.pmf.sc.gov.br/sistemas/' \
              'saude/unidades_saude/populacao/'

    summaryURL = 'uls_2013_index.php'
    floripaPage = PageContent(mainURL + summaryURL)

    # create database
    db = dbm.DbManager()

    areas = db.select_area()

    file_name = 'dados_2013/dados_2013.csv'
    with open(file_name, 'w') as f:
        f.write('Codigo da Area,Residentes,Homens Residentes,Mulheres Residentes\n')

        for area in areas:
            bairro = db.select_bairro(area[4])

            bairro = bairro[1].encode('utf-8')
            print "bairro: ", bairro

            # get area page
            area_page = PageContent(area[3])

            area_table = area_page.getTables()[0]

            # get table rows
            rows = area_table.find_all('tr')
            # for i in range(len(rows)):
            row = rows[-1]
            cols = row.find_all('td')
            data_line = list()
            for j in range(len(cols)):
                if 0 < j < 4:
                    col = cols[j]
                    content = col.contents
                    for c in content:
                        word = c.encode('utf-8')
                        word = word.strip()
                        word = word.replace('<b>', '')
                        word = word.replace('</b>', '')
                        data_line.append(word)
            area_data = area[1] + ',' + ','.join(data_line)
            clean_line = area_data
            f.write(clean_line + '\n')

    db.close()

"""

tables = floripaPage.getTables()

rows = list()
trs = tables[1].find_all('tr')
for tr in reversed(trs):
    rows.append(tr)
    # print len(tr.contents)
    # print tr.contents

for row in rows:
    links = row.find_all('a')
    regiao = None
    bairro = None

    for i in range(len(links)):
        link = mainURL + links[i]['href']
        nome = unicode(links[i].string)

        if i == 0:  # insert regiao
            print 'update_regiao'
            db.update_regiao(nome, '2013', link)
            regiao = unicode(nome)
        elif i == 1:  # insert bairro
            print 'update_bairro'
            db.update_bairro(nome, '2013', link, regiao)
            bairro = nome
        elif nome.isdigit():
            print 'update_area'
            db.update_area(nome, '2013', link, bairro)
        else:
            break

        print "link: ", links[i]
        print
    print '-----------------------------------------------'
db.print_regiao()
print
db.print_bairro()
print
db.print_area()
print
"""
