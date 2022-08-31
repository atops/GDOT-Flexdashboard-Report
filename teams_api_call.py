# -*- coding: utf-8 -*-
"""
Created on Wed Jun 15 12:07:04 2022

@author: Alan.Toppen
"""
from suds.client import Client

url = 'http://www.designitapps.com/GDOT/TEAMSService.svc?wsdl'
client = Client(url, username='gdot_it', password='Pass@word1')

client.service.IsUserLoggedIn()
# Exception: (415, "Cannot process the message because the content type 'text/xml; charset=utf-8' was not the expected type 'application/soap+msbin1'.")




from requests import Session
from requests.auth import HTTPBasicAuth
from zeep import Client
from zeep.transports import Transport

url = 'http://www.designitapps.com/GDOT/TEAMSService.svc?wsdl'
session = Session()
session.auth = HTTPBasicAuth('gdot_it', 'Pass@word1')
client = Client(url, transport=Transport(session=session))

client.wsdl
client.service
# Not sure where to go from here.

