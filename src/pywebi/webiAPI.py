from __future__ import annotations
import requests
import json
import xml.etree.ElementTree as ET

class WEBIAPI:
    
    C_URL_BASE = "http://{serverIP}:{port}"
    C_URL_LOGON = "/biprws/logon/long"
    C_URL_DOCUMENTS = "/biprws/raylight/v1/documents?offset={offset}&limit={limit}"
    C_URL_DOCUMENT_PROPERTIES = "/biprws/raylight/v1/documents/{documentId}/properties"
    C_URL_DOCUMENT_DETAILS = "/biprws/raylight/v1/documents/{documentId}"
    C_URL_DOCUMENT_DATAPROVIDERS = "/biprws/raylight/v1/documents/{documentId}/dataproviders"
    C_URL_DOCUMENT_SCHEDULES = "/biprws/raylight/v1/documents/{documentId}/schedules"
    C_URL_UNIVERSE_DETAIL = "/biprws/raylight/v1/universes/{universeId}"   
    C_URL_DOCUMENT_DATAPROVIDER_EXPR = "/biprws/raylight/v1/documents/{documentId}/dataproviders/{dataProviderId}"
        
    def __init__(self,serverIP,port):
        self.__serverIP = serverIP
        self.__port = port 
        self.__logonToken = None 
        pass 
    
    def __get_url_base(self):
        return WEBIAPI.C_URL_BASE.format(serverIP = self.__serverIP,port = self.__port)
    
    def __get_url_logon(self):
        return self.__get_url_base() + WEBIAPI.C_URL_LOGON
    
    def __get_url_documents(self,offset = 0, limit = 50):
        urlBase = self.__get_url_base()
        urlDocs = WEBIAPI.C_URL_DOCUMENTS.format(offset = offset, limit = limit)
        return urlBase + urlDocs
    
    def __get_url_document_properties(self,documentId):
        urlBase = self.__get_url_base()
        urlProp = WEBIAPI.C_URL_DOCUMENT_PROPERTIES.format(documentId = documentId)
        return urlBase + urlProp
    
    def __get_url_document_dataproviders(self,documentId):
        urlBase = self.__get_url_base()
        urlDataProv = WEBIAPI.C_URL_DOCUMENT_DATAPROVIDERS.format(documentId = documentId)
        return urlBase + urlDataProv    
    
    def __get_url_document_dataprovider_expr(self,documentId,dataProviderId):
        urlBase = self.__get_url_base()
        urlExpr = WEBIAPI.C_URL_DOCUMENT_DATAPROVIDER_EXPR.format(documentId = documentId, dataProviderId = dataProviderId)
        return urlBase + urlExpr       
    
    def __get_url_universe(self,universeId):
        urlBase = self.__get_url_base()
        urlUniverse = WEBIAPI.C_URL_UNIVERSE_DETAIL.format(universeId = universeId)
        return urlBase + urlUniverse        
    
    def __get_url_document_schedules(self,documentId):
        urlBase = self.__get_url_base()
        urlSchedules = WEBIAPI.C_URL_DOCUMENT_SCHEDULES.format(documentId = documentId)
        return urlBase + urlSchedules      
    
    def __get_url_document_details(self,documentId):
        urlBase = self.__get_url_base()
        urlDet = WEBIAPI.C_URL_DOCUMENT_DETAILS.format(documentId = documentId)
        return urlBase + urlDet   
    
    def __getLogonHeader(self):
        if self.__logonToken:
            headers = {
                "X-SAP-LogonToken": self.__logonToken,
                "Accept": "application/json",
                "Content-Type": "application/json",
            }
        else: 
            raise Exception("[WEBIAPI] You must first run logon method to get a valid logok tocken")
        
        return headers
    
    def __requests_withHeaders(self,api_url):
        return requests.get(api_url,headers = self.__getLogonHeader())
    
    def getDocumentProperties(self,documentId):
    
        api_url = self.__get_url_document_properties(documentId)
        response = self.__requests_withHeaders(api_url)
        doc_prop_json = json.loads(response.text)  
        
        properties = doc_prop_json.get("properties",None)
        if properties: 
            property = properties.get("property",None)
            if property:
                return property
            else: 
                print(doc_prop_json)
                raise Exception(f"[WEBIAPI.getDocumentProperties] documentId = {documentId} - 'property' not found")
        else: 
            print(doc_prop_json)
            raise Exception(f"[WEBIAPI.getDocumentProperties] documentId = {documentId} - 'properties' not found")
        
    def getDocumentDataProviders(self,documentId):
    
        api_url = self.__get_url_document_dataproviders(documentId)
        response = self.__requests_withHeaders(api_url)
        doc_prov_json = json.loads(response.text)   
        return doc_prov_json        
    
    def getDocumentDataProviderExpr(self,documentId,dataProviderId):
    
        api_url = self.__get_url_document_dataprovider_expr(documentId,dataProviderId)
        response = self.__requests_withHeaders(api_url)
        doc_provexpr_json = json.loads(response.text)   
        return doc_provexpr_json        

    def getDocumentSchedules(self,documentId):
    
        api_url = self.__get_url_document_schedules(documentId)
        response = self.__requests_withHeaders(api_url)
        doc_sched_json = json.loads(response.text)   
        return doc_sched_json       

   
    def getUniverseDetails(self,universeId):    
        api_url = self.__get_url_universe(universeId)
        response = self.__requests_withHeaders(api_url)
        univ_json = json.loads(response.text)   
        return univ_json 
            
    def getDocumentDetails(self,documentId):    
        api_url = self.__get_url_document_details(documentId)
        response = self.__requests_withHeaders(api_url)
        doc_det_json = json.loads(response.text)   
        return doc_det_json  
    
    def getAllDocuments(self):
        limit = 50
        offset = 0
        max_limit = 5000
        lst_all_docs = []
        
        while offset < max_limit:
            api_url = self.__get_url_documents(offset,limit)
            response = self.__requests_withHeaders(api_url)
            json_resp = json.loads(response.text)
                        
            if "error" in json_resp.keys():
                if json_resp["error"]["error_code"] == "WSR 00400":
                    #out of bounds
                    break
                else:
                    error_code = json_resp['error']['error_code']
                    error_message = json_resp['error']['message']
                    raise Exception(f"Error [{error_code}] {error_message}")
            else:
                lst_docs = json_resp["documents"]["document"]        
                if len(lst_all_docs) == 0:
                    lst_all_docs = lst_docs 
                else: 
                    lst_all_docs.extend(lst_docs)
                offset = offset + limit

        return lst_all_docs 
    
    def logon(self,username,password):
        # Create the request payload
        payload = {
            "userName": username,
            "password": password
        }
        api_url = self.__get_url_logon()
        response = requests.post(api_url, json=payload)
        #print(response.text)
        
        # Extract the logon token from the response
        root = ET.fromstring(response.text)
        #root = ET.fromstring('<entry xmlns="http://www.w3.org/2005/Atom"><author><name>@arssapbi01p.ccno.coop.it:6400</name></author><title type="text">Logon Result</title><updated>2023-12-08T15:34:10.153Z</updated><content type="application/xml"><attrs xmlns="http://www.sap.com/rws/bip"><attr name="logonToken" type="string">10.58.19.85:6400@{3&amp;2=575842,U3&amp;2v=10.58.19.85:6400,UP&amp;66=40,U3&amp;68=secEnterprise:Administrator,UP&amp;S9=12,U3&amp;qe=100,U3&amp;vz=FXRagVlKrj803xSCBrQ9bkaKioms.j.FUfBDNTPYSiA4mM.Tb6Lob_86eRbXB0XG,UP}</attr></attrs></content></entry>')
        namespaces = {
            "atom": "http://www.w3.org/2005/Atom",
            "bip": "http://www.sap.com/rws/bip"
        }

        # Find the logonToken element
        logon_token_element = root.find(".//bip:attr[@name='logonToken']", namespaces)    

        # Extract the logon token value
        logon_token = logon_token_element.text  
        self.__logonToken = logon_token  