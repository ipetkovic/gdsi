#-------------------------------------------------------------------------------
# Name:        module1
# Purpose:
#
# Author:      kovacicm
#
# Created:     18/04/2016
# Copyright:   (c) kovacicm 2016
# Licence:     <your licence>
#-------------------------------------------------------------------------------
import requests
import xml.etree.ElementTree as ET
import json
import string
from datetime import datetime
import csv

def ws_get(last_id):


    url="http://tenisliga.com/ws/public.asmx?WSDL"
    #headers = {'content-type': 'application/soap+xml'}
    headers = {'content-type': 'text/xml'}
    body1 = """<?xml version="1.0" encoding="utf-8"?>
<soap:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
  <soap:Body>
    <SusretJSON xmlns="http://tempuri.org/">
      <susretId>int</susretId>
    </SusretJSON>
  </soap:Body>
</soap:Envelope>"""
    body2 = """<?xml version="1.0" encoding="utf-8"?>
<soap:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
  <soap:Body>
    <SusretIgracJSON xmlns="http://tempuri.org/">
      <susretId>int</susretId>
    </SusretIgracJSON>
  </soap:Body>
</soap:Envelope>"""

    lastid1 = str(1);
    lastid2 = lastid1;
    check = 0
    susreti = []
    susretigrac = []

    templine = ['SusretId','SezonaId','LigaId','Datum','Lokacija','PrijavioId','NijeOdigran','TkoJeRezervirao','DomacinLoptice','RazlogNeigranja']
    susreti.append(templine)
    templine2 = ['SusretIgracId','SusretId','IgracId','Set1','Set2','Set3','Pobjednik']
    susretigrac.append(templine2)

    while check == 0:

        body = string.replace(body1,'int',lastid1)
        response = requests.post(url,data=body,headers=headers)
        root = ET.fromstring(response.content)
        data = json.loads(root.find('.//{http://tempuri.org/}SusretJSONResult').text)


        for d in data:
            tempdate = d['Datum']
            start = tempdate.find('/Date(') + 6
            end = tempdate.find(')/', start)
            newdate = tempdate[start:end]
            dtobject = datetime.fromtimestamp(float(newdate) / 1000.0)
            dtstring = str(dtobject.day)+'.'+ str(dtobject.month)+'.'+str(dtobject.year)+'.'+' 00:00:00'
            print dtstring
            templine = [d['SusretId'],d['SezonaId'],d['LigaId'],dtstring,'lokacija',d['PrijavioId'],d['NijeOdigran'],d['TkoJeRezervirao'],d['DomacinLoptice'],'razlog']
            lastid1 = str(d['SusretId']+1)
            if lastid1 == str(last_id+1):
                check = 1
            susreti.append(templine)

        body = string.replace(body2,'int',lastid2)
        response = requests.post(url,data=body,headers=headers)
        root = ET.fromstring(response.content)
        data = json.loads(root.find('.//{http://tempuri.org/}SusretIgracJSONResult').text)

        for d in data:
            if d['Pobjednik'] == True:
                res = '1'
            else:
                res = '0'
            templine2 = [d['SusretIgracId'],d['SusretId'],d['IgracId'],d['Set1'],d['Set2'],d['Set3'],res]
            lastid2 = lastid1;
            susretigrac.append(templine2)


    with open('Susret.csv', 'wb') as myfile:
        wr = csv.writer(myfile)
        wr.writerows(susreti)
    with open('SusretIgrac.csv', 'wb') as myfile:
        wr = csv.writer(myfile)
        wr.writerows(susretigrac)
#    print response.content
    pass

if __name__ == '__main__':
    main()
