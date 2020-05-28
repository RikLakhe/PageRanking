import sqlite3
import urllib.error
import ssl
from urllib.parse import urljoin
from urllib.parse import urlparse
from urllib.request import urlopen
from bs4 import BeautifulSoup


class spider:
    # setup from spidering
    # Ignore SSL certificate errors
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE

    # set up database
    con = sqlite3.connect('spider2.sqlite')
    cur = con.cursor()

    cur.executescript('''
        create table if not exists Pages(id INTEGER PRIMARY KEY, url TEXT UNIQUE, html TEXT,error INTEGER, old_rank REAL, new_rank REAL);
        create table if not exists Links(from_id INTEGER, to_id INTEGER);
        create table if not exists Webs(url TEXT UNIQUE);
    ''')

    # part 1
    cur.execute('select url,id from Pages where html is null and error is null order by random() limit 1')
    row = cur.fetchone()
    if row is not None:
        print("Restarting existing crawl.  Remove spider.sqlite to start a fresh crawl.")
    else:
        starturl = input('Enter new url to spider')

        if len(starturl) < 1: starturl = 'http://python-data.dr-chuck.net/'
        if starturl.endswith("/"): starturl = starturl[:-1]
        if starturl.endswith(".html") or starturl.endswith(".htm"):
            pos = starturl.rfind("/")
            starturl = starturl[:pos]

        web = starturl
        if len(web) > 1:
            cur.execute('insert or ignore into Webs (url) values (?)', (web,))
            cur.execute('insert or ignore into Pages (url,html,new_rank) values (?,null,1.0)', (web,))
            con.commit()

    # print all url in webs
    cur.execute('select url from Webs')
    webs = list()
    for row in cur:
        webs.append(str(row[0]))
    print(webs)

    # part3 spidering starting loop
    many = 0
    while True:
        # part 4 take number of pages to spider
        if many < 1:
            numberPages = input('Enter number of pages:')
            if len(numberPages) < 1: break
            many = int(numberPages)
        many = many - 1
        # print(many)

        # part 5 take a url from pages
        try:
            cur.execute('select id,url from Pages where html is null and error is null order by random() limit 1')
            row = cur.fetchone()
            from_id = row[0]
            url = row[1]
            # print('here', from_id, url)
            # remove all the link from this page in link
            cur.execute('delete from Links where from_id = ?', (from_id,))
        except:
            print('No retrievable HTML page found')
            many = 0
            break

        print(from_id, url, end=' ')

        # part 6 urlopen the url and get response from web
        try:
            document = urlopen(url, context=ctx)
            # print('response data', document.getcode(), document.info().get_content_type())
            if document.getcode() != 200:
                print("Error on page")
                cur.execute('update Pages set error=? where url=?', (document.getcode(), url,))
                continue
            if document.info().get_content_type() != 'text/html':
                print("Ignore non text/html page")
                cur.execute('delete from Pages where url=?', (url,))
                continue

            html = document.read()
            print('(' + str(len(html)) + ')', end=' ')
            soup = BeautifulSoup(html, 'html.parser')
        except:
            print("Unable to retrieve or parse page")
            cur.execute('update Pages set error=-1 where url=?', (url,))
            con.commit()
            continue

        cur.execute('insert or ignore into Pages (url,html,new_rank) values (?,null,1.0)', (url,))
        cur.execute('update Pages set html=? where url=?', (memoryview(html), url,))
        # part 7 going through a tags to get href
        count = 0
        tags = soup('a')
        for tag in tags:
            href = tag.get('href', None)

            if href is None: continue
            # handling /content
            up = urlparse(href)
            if len(up.scheme) < 1: href = urljoin(url, href)
            # handling "#"
            ipos = href.find("#")
            if ipos > 1: href = href[:ipos]
            if href.endswith('.png') or href.endswith('.gif') or href.endswith('.jpg') or href.endswith(
                    '.jpeg'): continue
            if href.endswith("/"): href = href[:-1]
            if len(href) < 1: continue

            # print('href got is ', href)

            # Check if the URL is in any of the webs
            found = False
            for web in webs:
                if (href.startswith(web)):
                    found = True
                    break
            if not found: continue

            cur.execute('INSERT OR IGNORE INTO Pages (url, html, new_rank) VALUES ( ?, NULL, 1.0 )', (href,))
            count = count + 1
            con.commit()

            # part 7 add in links from id to to id
            cur.execute('SELECT id FROM Pages WHERE url=? LIMIT 1', (href,))
            try:
                row = cur.fetchone()
                to_id = row[0]
            except:
                print('Could not retrieve id')
                continue
            cur.execute('INSERT OR IGNORE INTO Links (from_id, to_id) VALUES ( ?, ? )', (from_id, to_id))

        print(count)

    cur.close()
