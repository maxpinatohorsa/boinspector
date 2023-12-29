#from pywebi.webiAPI import WEBIAPI
from pywebi.webiModel import * 
from pywebi.webiDB import * 
import argparse 

def writeWebiDocToDB(wdoc : WEBIDoc,webiDB : WEBIDB):
    webiDB.insertWEBIDoc(wdoc)
    for dprov in wdoc.getDataProviders():
        webiDB.insertWEBIDataProv(wdoc,dprov)

        if dprov.getUniverse() is not None:
            webiDB.insertWEBIUniverse(dprov.getUniverse())
        
        for expr in dprov.getExpression():
            webiDB.insertWEBIDataProvExpr(wdoc,dprov,expr)
            
    for sched in wdoc.getSchedules():
        webiDB.insertWEBISchedule(wdoc,sched)

def readAllWebiDocsAndDetails(webiDB : WEBIDB, dictUniv) -> dict[int,WEBIDoc]:
    #read all data providers
    dictExpr = webiDB.readAllWEBIDPExpr()
    dictDP = webiDB.readAllWEBIDataProviders(dictExpr,dictUniv)
    dictSched = webiDB.readAllWEBISchedules()
    dictWDocs = webiDB.readAllWEBIDocs(dictDP,dictSched)
    return dictWDocs

def print_title():
    print("------------------------------------------------")
    print(" BO Inspector")
    print("------------------------------------------------")
    print()

if __name__ == "__main__":
    
    print_title()
    
    
    parser = argparse.ArgumentParser(description = "Argomenti")
    parser.add_argument("host",type=str,help="BO Host")
    parser.add_argument("port",type=str,help="BO Port for REST API (defaut = 6405)")
    parser.add_argument("user",type=str,help="User")    
    parser.add_argument("password",type=str,help="Password")    

    args = parser.parse_args()
    server = args.host 
    port = args.port
    username = args.user
    password = args.password
    
    #save all on a local file.
    filename = "webiresources.db"
    
    webiDB = WEBIDB(filename)
    webiDB.createDB_ifNotExists()
       
    limit = 50
    offset = 0
    max_limit = 2500
    
    #before start read all data from db
    print("Read univese and documents from cache DB ... ")
    cache_univ = webiDB.readAllUniverse()
    cache_wdoc = readAllWebiDocsAndDetails(webiDB,cache_univ)

    #now i read the list of all documents from bobj    
    lst_all_docs = [] 
    print("Get all documents from BOBJ ...")
    
    webiAPI = WEBIAPI(server,port)
    webiAPI.logon(username,password)        
    lst_all_docs_api = webiAPI.getAllDocuments()
    print(f"Docs: [{len(lst_all_docs_api)}]")
    idx = 1
        
    for doc in lst_all_docs_api:
        
        print(f"Process doc [{idx}] ...")
        wdoc = WEBIDoc.CreateByJson(doc)
        
        #if the wdoc already exists, continue
        wdocId = int(wdoc.getId())
        print(f"  --> WEBI ID [{wdoc.getId()}]")
        if wdocId not in cache_wdoc.keys():
            
            print("  --> Read from API ...")
        
            #read properties and load to instance.
            try:
                prop = webiAPI.getDocumentProperties(wdoc.getId())
                WEBIDoc.LoadPropertiesByJson(prop,wdoc)
                wdoc.setPropertiesFound(True)
                #print("  --> Properties")
            except Exception as e:
                wdoc.setPropertiesFound(False)
                print("  --> Properties ... NOT FOUND")
            #read details and load to instance 
            dets = webiAPI.getDocumentDetails(wdoc.getId())
            WEBIDoc.LoadDetailsByJson(dets,wdoc)
            #print("  --> Details")
            #read all the data providers 
            try:
                dataProvAPI = webiAPI.getDocumentDataProviders(wdoc.getId())
                dataProvLst = WEBIDataProvider.GetListByJson(dataProvAPI)
                wdoc.setDataProviders(dataProvLst)
                #print("  --> Data Providers")
            except Exception as e:
                print(e)
                print("  --> Data Providers (ERROR)")
                
            #for each data providers, if the data providers type 
            #is UNIVERSE, I read the Universe Detail.

            for prov in wdoc.getDataProviders():
                if prov.getDataSourceType() in ( WEBIDataProvider.C_DSTYPE_UNX, WEBIDataProvider.C_DSTYPE_UNV):
                    try:
                        univId = int(prov.getDataSourceId())
                        univ = cache_univ.get(univId,None)
                        if univ is None:
                            univAPI = webiAPI.getUniverseDetails(prov.getDataSourceId())
                            univ = WEBIUniverse.CreateByJson(univAPI)
                            #print(f"  --> Read Universe [{prov.getDataSourceId()}]")
                            cache_univ[prov.getDataSourceId()] = univ 
                        prov.setUniverse(univ)
                    except Exception as e:
                        print(e)
                        print("  --> Read Univers Detail Error")
                #get expression
                try: 
                    dpexpAPI = webiAPI.getDocumentDataProviderExpr(wdoc.getId(),prov.getId())
                    prov.setExpression(WEBIDataProviderExpr.GetListByJson(dpexpAPI))
                    #print("  --> Data Provider Expression OK")
                except Exception as e:
                    print(e)
                    print(f"  --> DOC ID [{wdoc.getId()}] DP ID [{prov.getId()}] Expression (ERROR)")    
                    print(dpexpAPI)           
                

            #read all schedules
            try:
                schedAPI = webiAPI.getDocumentSchedules(wdoc.getId())
                schedLst = WEBISchedule.GetListByJson(schedAPI)
                wdoc.setSchedules(schedLst)
                #print("  --> Schedules")
            except Exception as e:
                print(e)
                print(f"  --> DOC ID [{wdoc.getId()}] Schedules (ERROR)")    
                print(schedAPI)
                
            #write wdoc to DB
            writeWebiDocToDB(wdoc,webiDB)
            print("  --> Write to DB OK")
                      
            #add wdoc to lst_all_docs
            lst_all_docs.append(wdoc)
            cache_wdoc[wdocId] = wdoc
        else:
            print("  --> Already on DB")
            
        idx = idx + 1
        if idx > max_limit:
            print(f"BREAK AFTER {max_limit} ITERACTIONS !!!")
            break 

    print()
    webiDB.closeConnection()
    print()    
    print(f"Il file [{filename}] contiene il dettaglio di BO")
    print("Operazione Completata")    
          
