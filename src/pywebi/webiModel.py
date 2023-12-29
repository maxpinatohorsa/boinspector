from __future__ import annotations
import requests
import json
import xml.etree.ElementTree as ET

from pywebi.webiAPI import WEBIAPI


class WEBIDataProviderExpr:
    pass 

class WEBIDataProviderExpr:
    
    def __init__(self,id,name,dataSourceObjectId,
                 dataType,qualification,dataSourceEnriched,formulaLanguageId,
                 dataSourcePath):
        self._id = id 
        self._name = name 
        self._dataSourceObjectId = dataSourceObjectId 
        self._dataType = dataType 
        self._qualification = qualification 
        self._dataSourceEnriched = dataSourceEnriched 
        self._formulaLanguageId = formulaLanguageId 
        self._dataSourcePath = dataSourcePath 
        
    def getDataSourcePath(self):
        return self._dataSourcePath
        
    def getFormulaLanguageId(self):
        return self._formulaLanguageId
        
    def getDataSourceEnriched(self):
        return self._dataSourceEnriched
        
    def getQualification(self):
        return self._qualification
        
    def getId(self):
        return self._id 
    
    def getName(self):
        return self._name
    
    def getDataSourceObjectId(self):
        return self._dataSourceObjectId
    
    def getDataType(self):
        return self._dataType

    @staticmethod
    def GetListByJson(json_dprovdet) -> list(WEBIDataProviderExpr):
        
        try:
        
            dataprovider = json_dprovdet.get("dataprovider",None)
            dictionary = dataprovider.get("dictionary")
            expression = dictionary.get("expression")
            
            if expression is not None:  
                lst = []
                for expr in expression:        
                    id = expr.get("id",None)
                    name = expr.get("name",None)
                    dataSourceObjectId = expr.get("dataSourceObjectId",None)
                    dataType = expr.get("@dataType",None)
                    qualification = expr.get("@qualification",None)
                    dataSourceEnriched = expr.get("@dataSourceEnriched",None)
                    formulaLanguageId = expr.get("formulaLanguageId",None)
                    dataSourcePath = expr.get("dataSourcePath",None)
                    lst.append(WEBIDataProviderExpr(id,name,dataSourceObjectId,
                        dataType,qualification,dataSourceEnriched,formulaLanguageId,
                        dataSourcePath))
                
                return lst
            
        except Exception as e:
            print("Error on WEBIDataProviderExpr.GetListByJson")
            print(json_dprovdet)
            raise Exception("[WEBIDataProviderExpr.GetListByJson] Error" + str(e))

class WEBIUniverse:
    pass 

class WEBIUniverse:
    
    def __init__(self, id, cuid, name, description, type, subType,path,connected):
        self._id = id 
        self._cuid = cuid 
        self._name = name 
        self._description = description 
        self._type = type 
        self._subType = subType 
        self._path = path 
        self._connected = connected 
    
    
    def getId(self):
        return self._id 
    
    def getCUID(self):
        return self._cuid    
    
    def getName(self):
        return self._name 
    
    def getDescription(self):
        return self._description
    
    def getType(self):
        return self._type
        
    def getSubType(self):
        return self._subType
        
    def getPath(self):
        return self._path
        
    def getConnected(self):
        return self._connected
        
    @staticmethod
    def CreateByJson(json_univ) -> WEBIDoc:
        univ = json_univ.get("universe",None)
        
        if univ:         
            id = univ.get("id",None)
            cuid = univ.get("cuid",None)
            name = univ.get("name",None)
            description = univ.get("description",None)
            type = univ.get("type",None)
            subType = univ.get("subType",None)
            path = univ.get("path",None)
            connected = univ.get("connected",None)
            return WEBIUniverse(id,cuid,name,description,type,subType,path,connected)
        else:
            print(json_univ)
            raise Exception("WEBIUniverse.CreateByJson - Property 'universe' not found")        
        

   
class WEBISchedule:
    pass 

class WEBISchedule:
    
    def __init__(self,id,name,format,status,updated):
        self._id = id 
        self._name = name 
        self._format = format 
        self._status = status 
        self._updated = updated 
        
    def getId(self):
        return self._id 
        
    def getName(self):
        return self._name
        
    def getFormat(self):
        return self._format
        
    def getStatus(self):
        return self._status
        
    def getUpdated(self):
        return self._updated

    def __str__(self):
        output = f"""  
--- Schedule [{self._id}] ---------------------------------
  Name:                 [{self._name}]
  Format:         [{self._format}]
  Status:       [{self._status}]
  Updated:   [{self._updated}]
        """                    
        return output 
        
    @staticmethod
    def GetListByJson(json_sched) -> list(WEBISchedule):
        lst = []
        try:
            schedules = json_sched.get("schedules",None)
            if schedules: 
                schedule = schedules.get("schedule",None)
                if schedule: 
                    for sc in schedule:
                        id = sc.get("id",None)
                        name = sc.get("name",None)
                        format = sc["format"]["@type"]
                        status = sc["status"]["$"]
                        updated = sc.get("updated",None)
                        wSched = WEBISchedule(id,name,format,status,updated)
                        lst.append(wSched)
                    return lst 
                elif schedule is not None:
                    return lst 
                else: 
                    print(json_sched)
                    raise Exception("WEBIDataProvider.GetListByJson - Property 'schedule' not found")
            else:
                print(json_sched)
                raise Exception("WEBIDataProvider.GetListByJson - Property 'schedules' not found")
            
        except Exception as e:
            raise e
    
class WEBIDataProvider:
    pass 

class WEBIDataProvider: 
    
    C_DSTYPE_UNX = "unx"
    C_DSTYPE_UNV = "unv"
    
    def __init__(self,id,name,dataSourceId,dataSourceType,dataSourceLocation,updated,isPartial,rowCount):
        self._id = id 
        self._name = name 
        self._dataSourceId = dataSourceId 
        self._dataSourceType = dataSourceType 
        self._dataSourceLocation = dataSourceLocation 
        self._updated = updated 
        self._isPartial = isPartial 
        self._rowCount = rowCount 
        #univerfse
        self._universe = None
        #expression
        self._expression = [] 
        
    def getId(self) -> str:
        return self._id 
        
    def getName(self):
        return self._name 
    
    def getDataSourceId(self) -> str: 
        return self._dataSourceId    

    def getDataSourceType(self) -> str:
        return self._dataSourceType
    
    def getDataSourceLocation(self):
        return self._dataSourceLocation
    
    def getUpdated(self):
        return self._updated
    
    def getIsPartial(self):
        return self._isPartial
    
    def getRowCount(self):
        return self._rowCount
    
    def getUniverse(self):
        return self._universe
    
    def getExpression(self):
        return self._expression
        
    def setUniverse(self,univ : WEBIUniverse):
        self._universe = univ 
    
    def setExpression(self,expr : list(WEBIDataProviderExpr)):
        self._expression = expr 
        
    def __str__(self):
        
        output_universe = "No Universe"
        if self._universe:
            output_universe = f"""
  Universe [{self._universe._id}]
    Cuid:           [{self._universe._cuid}]
    Name:           [{self._universe._name}]
    Description:    [{self._universe._description}]
    Path:           [{self._universe._path}]
    Type:           [{self._universe._type}]
    SubType:        [{self._universe._subType}]
    Connected:      [{self._universe._connected}]
                
            """        
        
        output = f"""  
--- Data Prov [{self._id}] ---------------------------------
  Name:                 [{self._name}]
  DataSourceId:         [{self._dataSourceId}]
  DataSourceType:       [{self._dataSourceType}]
  DataSourceLocation:   [{self._dataSourceLocation}]
  Updated:              [{self._updated}]
  IsPartial:            [{self._isPartial}]
  RowCount:             [{self._rowCount}]    
  
{output_universe}    
        """                    
        return output 
        
        
    @staticmethod
    def GetListByJson(json_prov) -> list(WEBIDataProvider):
        lst = []
        try:
            dataproviders = json_prov.get("dataproviders",None)
            if dataproviders: 
                dataprovider = dataproviders.get("dataprovider",None)
                if dataprovider: 
                    for prov in dataprovider:
                        id = prov.get("id",None)
                        name = prov.get("name",None)
                        dataSourceId = prov.get("dataSourceId",None)
                        dataSourceType = prov.get("dataSourceType",None)
                        dataSourceLocation = prov.get("dataSourceLocation",None)
                        updated = prov.get("updated",None)
                        isPartial = prov.get("isPartial",None)
                        rowCount = prov.get("rowCount",None)
                        wProv = WEBIDataProvider(id,name,dataSourceId,dataSourceType,dataSourceLocation,updated,isPartial,rowCount)
                        lst.append(wProv)
                    return lst 
                else: 
                    raise Exception("WEBIDataProvider.GetListByJson - Property 'dataprovider' not found")
            else:
                raise Exception("WEBIDataProvider.GetListByJson - Property 'dataproviders' not found")
            
        except Exception as e:
            raise e

class WEBIDoc:
    pass 
class WEBIDoc:
    
    def __init__(self,id,cuid,name,description,folderId,scheduled):
        self._id = id 
        self._cuid = cuid 
        self._name = name 
        self._description = description
        self._folderId = folderId 
        self._scheduled = scheduled 
        #properties
        self._propertiesFound = False 
        self._lastRefreshDate = None 
        self._modificationDate = None 
        self._documentType = None 
        self._creationDate = None 
        #details
        self._path = None 
        self._updated = None 
        #data providers 
        self._dataProviders = []
        #data schedules 
        self._schedules = []
        
        
    def getId(self):
        return self._id 
        
    def getCUID(self):
        return self._cuid
        
    def getName(self):
        return self._name 
        
    def getDescription(self):
        return self._description
        
    def getFolderId(self):
        return self._folderId
        
    def getScheduled(self):
        return self._scheduled
        
    def getPropertiesFound(self):
        return self._propertiesFound
        
    def getLastRefreshDate(self):
        return self._lastRefreshDate
        
    def getModificationDate(self):
        return self._modificationDate
        
    def getDocumentType(self):
        return self._documentType
        
    def getCreationDate(self):
        return self._creationDate
        
    def getPath(self):
        return self._path
        
    def getUpdated(self):
        return self._updated
        
    def getDataProviders(self) -> list(WEBIDataProvider):
        return self._dataProviders
    
    def getSchedules(self) -> list(WEBISchedule):
        return self._schedules
        
    def setDataProviders(self, dataProviders : list(WEBIDataProvider)):
        self._dataProviders = dataProviders
        
    def setSchedules(self,schedules : list(WEBISchedule)):
        self._schedules = schedules
        
    def __str__(self):
        
        
        output_properties = "  Properties Not Found" if not self._propertiesFound else f"""
  Last Refresh Date:            [{self._lastRefreshDate}]
  Modification Date:            [{self._modificationDate}]
  Document Type:                [{self._documentType}]
  Creation Date:                [{self._creationDate}]        
        """
        
        output_dataProviders = "" 
        for wprov in self._dataProviders:
            output_dataProviders += str(wprov) + "\n"
            
        output_schedules = ""
        for wsched in self._schedules:
            output_schedules += str(wsched) + "\n" 
            
        output = f"""
=== WEBIDOC [{self._id}] ============================
  CUID:         [{self._cuid}]
  Name:         [{self._name}]
  Description:  [{self._description}]
  FolderId:     [{self._folderId}]
  Scheduled:    [{self._scheduled}]
--- Properties ---------------------------------------
{output_properties}
--- Details ------------------------------------------
  Path:                 [{self._path}]
  Updated:              [{self._updated}]

--- Data Providers -----------------------------------
{output_dataProviders}

--- Schedules ---------------------------------------
{output_schedules}                        
        """
        return output
        
    def getId(self):
        return self._id 
    
    def setPropertiesFound(self,propertiesFound):
        self._propertiesFound = propertiesFound
        
    
    @staticmethod
    def CreateByJson(json_doc) -> WEBIDoc:
        id = json_doc.get("id",None)
        cuid = json_doc.get("cuid",None)
        name = json_doc.get("name",None)
        description = json_doc.get("description",None)
        folderId = json_doc.get("folderId",None)
        scheduled = json_doc.get("scheduled",None)
        return WEBIDoc(id,cuid,name,description,folderId,scheduled)    
    
    @staticmethod
    def LoadPropertiesByJson(json_prop,wdoc : WEBIDoc):
        for prop in json_prop:            
            if prop.get("@key","-") == "lastrefreshdate": wdoc._lastRefreshDate = prop.get("$",None)
            if prop.get("@key","-") == "modificationdate": wdoc._modificationDate = prop.get("$",None)
            if prop.get("@key","-") == "documenttype": wdoc._documentType = prop.get("$",None)
            if prop.get("@key","-") == "creationdate": wdoc._creationDate = prop.get("$",None)
            
    @staticmethod
    def LoadDetailsByJson(json_det,wdoc : WEBIDoc):
        wdoc._path = json_det["document"].get("path",None)
        wdoc._updated = json_det["document"].get("updated",None)         