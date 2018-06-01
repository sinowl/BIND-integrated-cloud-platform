__author__ = 'yoonsu'

import os
from bs4 import BeautifulSoup as bs
import urllib
import re

class ExecuteCrawling:
	def __init__(self):
		#self.__beautifulsoup
		self.__beautifulsouplist=[]
		#self.__beautifulsoupResult		
		self.__webpages = set()
		self.__mainurl=''
		self.__numPage=0
        	pass

	def getcrawlingDatafromLocal(self,searchurl, makedir):
        	os.chdir("../../data/crawling/")
	        if(os.path.isdir(makedir)==False):
        	    os.mkdir(makedir)
	            os.chdir(makedir)
	        else:
        	    os.chdir(makedir)
	        searching = "curl "+searchurl
	        os.system(searching)

	def getUrlHTML(self, htmlurl ):
		html = urllib.urlopen( htmlurl )
		self.__beautifulsoup = bs( html.read(), "html.parser" ) 

	def getWebMain(self, totalurl):
		self.__urlsplit = totalurl.split('/')
		for item in self.__urlsplit:
			if item.find('www') != -1:
				return item

	def saveWebtoLocal(self, html):
		f = open('../../../data/crawling/'+str(self.__numPage)+'.txt', 'w')	
		f.write( str(html) )
		f.close()

	def crawlingallInWeb(self, weburl):
		html = urllib.urlopen( weburl )
		self.__beautifulsoup = bs( html.read(), "html.parser" ) 
		self.__beautifulsouplist.append( bs( html.read(), "html.parser" ) )
		self.__numPage += 1
		#self.saveWebtoLocal( self.__beautifulsoup )		
		if self.__mainurl == '':
			self.__mainurl = self.getWebMain( weburl )
		for link in self.__beautifulsoup.find_all( "a", href=re.compile("^\/") ):
			if 'href' in link.attrs:
				if link.attrs['href'] not in self.__webpages:
					newPage = 'http://'+self.__mainurl + link.attrs['href']
					ext = re.compile("(pdf)")
					m = ext.match( newPage )
					if m:
						continue
					else:
						print newPage
						self.__webpages.add(link.attrs['href'])
						self.crawlingallInWeb(newPage)
			self.getbeautifulSoups()

	def bsObjfindTags(self, tag ):
		self.__beautifulsoupResult = self.__beautifulsoup.find_all( tag )

	def getbeautifulSoups(self):
		return self.__beautifulsoup

if __name__ == '__main__':
	Crawl = ExecuteCrawling()
	Crawl.crawlingallInWeb( 'http://www.ehyundai.com/newPortal/NS/NS000001_L.do?branchCd=B00174000' )	
	Crawl.getbeautifulSoups()




