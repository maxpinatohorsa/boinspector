import sqlite3

from pywebi.webiModel import * 
from pywebi.bwModel import * 
from pathlib import Path


class WEBIDB:
    
    C_DB_PREPARATION_QUERYBEX_DROP = "drop table if exists BWQueryBEx"
    C_DB_PREPARATION_QUERYBEX = """
        create table if not exists BWQueryBEx(
            COMPID TEXT ,
            INFOCUBE TEXT ,
            MODTIME TEXT,
            REPTIME TEXT,
            PRIMARY KEY (COMPID)
            )
    
    """    

    C_DB_PREPARATION_DATAPROVIDER_EXPR_DROP = "drop table if exists WEBIDataProviderExpr"
    C_DB_PREPARATION_DATAPROVIDER_EXPR = """
        create table if not exists WEBIDataProviderExpr(
            WEBI_ID INTEGER ,
            DPROV_ID INTEGER ,
            ID INTEGER,
            DATA_SOURCE_OBJECT_ID TEXT,
            DATA_TYPE TEXT,
            QUALIFICATION TEXT,
            DATA_SOURCE_ENRICHED TEXT,
            FORMULA_LANGUAGE_ID TEXT,
            PRIMARY KEY (WEBI_ID,DPROV_ID,ID)
            )
    
    """

    C_DB_PREPARATION_WEBIDOCUMENT_DROP = "drop table if exists WEBIDocument"
    C_DB_PREPARATION_WEBIDOCUMENT = """
        create table if not exists WEBIDocument(
            ID INTEGER PRIMARY KEY,
            CUID TEXT, 
            NAME TEXT,
            DESCRIPTION TEXT,
            FOLDER_ID TEXT,
            SCHEDULED TEXT,
            PROPERTIES_FOUND TEXT,
            LASTREFRESH_DATE DATETIME,
            MODIFICATION_DATE DATETIME,
            DOCUMENT_TYPE TEXT,
            CREATION_DATE TEXT,
            PATH TEXT,
            UPDATED TEXT
            )
    """
    
    C_DB_PREPARATION_DATAPROVIDER_DROP = "drop table if exists WEBIDataProvider"
    C_DB_PREPARATION_DATAPROVIDER = """
        create table if not exists WEBIDataProvider(
            WEBI_ID INTEGER ,
            ID INTEGER ,
            NAME TEXT,
            DATA_SOURCE_ID TEXT,
            DATA_SOURCE_TYPE TEXT,
            DATA_SOURCE_LOCATION TEXT,
            UPDATED TEXT,
            ISPARTIAL TEXT,
            ROWCOUNT INTEGER,
            PRIMARY KEY (WEBI_ID,ID)
            )
    
    """
   
    C_DB_PREPARATION_SCHEDULES_DROP = "drop table if exists WEBISchedules"
    C_DB_PREPARATION_SCHEDULES = """
        create table if not exists WEBISchedules(
            WEBI_ID INTEGER ,
            ID INTEGER ,
            NAME TEXT,
            FORMAT TEXT,
            STATUS TEXT,
            UPDATED TEXT,
            PRIMARY KEY (WEBI_ID,ID)
            )
    
    """   
    
    C_DB_PREPARATION_UNIV_DROP = "drop table if exists WEBIUniverse"
    C_DB_PREPARATION_UNIV = """
        create table if not exists WEBIUniverse(
            ID INTEGER ,
            CUID TEXT ,
            NAME TEXT,
            DESCRIPTION TEXT,
            TYPE TEXT,
            SUB_TYPE TEXT,
            PATH TEXT,
            CONNECTED TEXT,
            PRIMARY KEY (ID)
            )
    
    """    
    
    
    def __init__(self,filename):
        self._filename = filename
        self._connection = None 
        self._cursor = None 
        
        
    def createConnection(self):        
        return sqlite3.connect(self._filename)
        
    def getConnection(self):
        if self._connection is None:
            self._connection = sqlite3.connect(self._filename)
                
        return self._connection
    
    def closeConnection(self):
        if self._connection:
            self._connection.close()
            self._connection = None 

    def createDB_BW(self):
        connection = self.getConnection()
        cursor = connection.cursor()
        cursor.execute(WEBIDB.C_DB_PREPARATION_QUERYBEX_DROP)
        cursor.execute(WEBIDB.C_DB_PREPARATION_QUERYBEX)
        
    def createDB_ifNotExists(self):
        file_path = Path(self._filename)
        if not file_path.exists():
            self.createDB()
    
    
    def createDB(self):
        connection = self.getConnection()
        cursor = connection.cursor()
        cursor.execute(WEBIDB.C_DB_PREPARATION_WEBIDOCUMENT_DROP)
        cursor.execute(WEBIDB.C_DB_PREPARATION_WEBIDOCUMENT)
        cursor.execute(WEBIDB.C_DB_PREPARATION_DATAPROVIDER_DROP)
        cursor.execute(WEBIDB.C_DB_PREPARATION_DATAPROVIDER)
        cursor.execute(WEBIDB.C_DB_PREPARATION_SCHEDULES_DROP)
        cursor.execute(WEBIDB.C_DB_PREPARATION_SCHEDULES)
        cursor.execute(WEBIDB.C_DB_PREPARATION_UNIV_DROP)
        cursor.execute(WEBIDB.C_DB_PREPARATION_UNIV)          
        cursor.execute(WEBIDB.C_DB_PREPARATION_DATAPROVIDER_EXPR_DROP)
        cursor.execute(WEBIDB.C_DB_PREPARATION_DATAPROVIDER_EXPR) 
                       
        cursor.execute(WEBIDB.C_DB_PREPARATION_QUERYBEX_DROP)
        cursor.execute(WEBIDB.C_DB_PREPARATION_QUERYBEX)
        
        self.closeConnection()


    def getAllRows(self,sql):
        conn = self.createConnection()
        cursor = conn.cursor()
        cursor.execute(sql)
        rows = cursor.fetchall()
        cursor.close()
        conn.close()
        return rows
    
    #its a dictionary with the list of all expression
    #by webi e dataprovider    
    def readAllWEBIDPExpr(self) -> dict[(int,int),WEBIDataProviderExpr]:
        sql = "select * from WEBIDataProviderExpr"
        rows = self.getAllRows(sql)
        dict = {}
        for row in rows:
            webiId = row[0]
            dprovId = row[1]
            id = row[2]
            dataSourceObjectId = row[3]
            dataType = row[4]
            qualification = row[5]
            dataSourceEnriched = row[6]
            formulaLanguageId = row[7]
            name = None
            expr = WEBIDataProviderExpr(id,name,dataSourceObjectId,dataType,
                    qualification,dataSourceEnriched,formulaLanguageId,None)
                                    
            eKey = (webiId,dprovId)
            if eKey in dict.keys():
                lst = dict[eKey]
                lst.append(expr)
            else:
                lst = []
                lst.append(expr)
                dict[eKey] = lst
            
        return dict             
        
    #this is a dictionary of list for all schedules
    #of webi
    def readAllWEBISchedules(self) -> dict[int,WEBISchedule]:
        sql = "select * from WEBISchedules"
        rows = self.getAllRows(sql)
        
        dict = {}
        for row in rows:
            webi_id = row[0]
            id = row[1]
            name = row[2]
            format = row[3]
            status = row[4]
            updated = row[5]
            sched = WEBISchedule(id,name,format,status,updated)
            
            if webi_id in dict.keys():
                dict[webi_id].append(sched)
            else:
                dict[webi_id] = [sched]
                        
        return dict        
        
    #its a dictionary with all data providers.
    def readAllWEBIDataProviders(self,dictExpr,dictUniv) -> dict[(int,int),WEBIDataProvider]:
        sql = "select * from WEBIDataProvider"
        rows = self.getAllRows(sql)
        dict = {}
        for row in rows:
            webi_id = row[0]
            id = row[1]
            name = row[2]
            dataSourceId = row[3]
            dataSourceType = row[4]
            dataSourceLocation = row[5]
            updated = row[6]
            isPartial = row[7]
            rowCount = row[8]
            
            dprov = WEBIDataProvider(id,name,dataSourceId,
                    dataSourceType,dataSourceLocation,updated,
                    isPartial,rowCount)
            #I add the expressions ... 
            eKey = (webi_id,id)
            lstExpr = dictExpr.get(eKey,None)
            if lstExpr:
                dprov.setExpression(dictExpr[eKey])
            #set the universes ..
            if dictUniv.get(dataSourceId,None):
                dprov.setUniverse(dictUniv[dataSourceId])
            
            
            if webi_id in dict.keys():
                dict[webi_id].append(dprov)
            else:
                dict[webi_id] = [dprov]
                                    
        return dict
    
    def readAllUniverse(self) -> dict[int,WEBIUniverse]:
        
        sql = "select * from WEBIUniverse"                        
        rows = self.getAllRows(sql)
        dict = {}
        for row in rows:
            id = row[0]
            cuid = row[1]
            name = row[2]
            description = row[3]
            type = row[4]
            subType = row[5]
            path = row[6]
            connected = row[7]
            univ = WEBIUniverse(id,cuid,name,description,type,subType,path,connected)
            dict[id] = univ
        
        return dict

    def readAllWEBIDocs(self,dictWEBIDataProvider,
                        dictWEBISChed) -> dict[int,WEBIDoc]:
        
        sql = "select * from WEBIDocument"                        
        rows = self.getAllRows(sql)
        dict = {}
        for row in rows:
            id = row[0]
            cuid = row[1]
            name = row[2]
            description = row[3]
            folderId = row[4]
            scheduler = row[5]
            wdoc = WEBIDoc(id,cuid,name,description,folderId,scheduler)
            wdoc.setDataProviders(dictWEBIDataProvider.get(id,None))
            wdoc.setSchedules(dictWEBISChed.get(id,None))
            
            dict[id] = wdoc
            
            
        
        return dict
            
        

    def insertWEBIDataProvExpr(self,wdoc : WEBIDoc,  dprov : WEBIDataProvider, dprovExpr: WEBIDataProviderExpr):
        sql = f"""insert into WEBIDataProviderExpr ( WEBI_ID , DPROV_ID , ID ,
            DATA_SOURCE_OBJECT_ID , DATA_TYPE , QUALIFICATION , DATA_SOURCE_ENRICHED ,
            FORMULA_LANGUAGE_ID ) 
            VALUES (?,?,?,?,?,?,?,?)              
            """              
        webiID = wdoc.getId()
        dprovID = dprov.getId()
        id = dprovExpr.getId()
        dataSourceObjectId = dprovExpr.getDataSourceObjectId()
        dataType = dprovExpr.getDataType()
        qualification = dprovExpr.getQualification()
        dataSourceEnriched = dprovExpr.getDataSourceEnriched()
        formulaLanguageId = dprovExpr.getFormulaLanguageId()
        
        values = (webiID,dprovID,id,dataSourceObjectId,dataType,qualification,dataSourceEnriched,formulaLanguageId)
        
        cursor = self.getConnection()
        cursor.execute(sql,values)
        self.getConnection().commit()     
 
    def insertWEBIUniverse(self,univ : WEBIUniverse):
        sql = f"""insert or ignore into WEBIUniverse ( ID , CUID , NAME ,
                DESCRIPTION , TYPE , SUB_TYPE , PATH , CONNECTED ) 
                VALUES (?,?,?,?,?,?,?,?)
               """       
                       
        id = univ.getId()
        cuid = univ.getCUID()
        name = univ.getName()
        description = univ.getDescription()
        type = univ.getType()
        subType = univ.getSubType()
        path = univ.getPath()
        connected = univ.getConnected()

        values = (id,cuid,name,description,type,
                  subType,path,connected)
        
        cursor = self.getConnection()
        cursor.execute(sql,values)
        self.getConnection().commit()   
        
    def insertWEBISchedule(self,wdoc : WEBIDoc,  sched : WEBISchedule):
        sql = f"""insert into WEBISchedules ( WEBI_ID ,ID  ,NAME,
            FORMAT, STATUS, UPDATED ) VALUES (?,?,?,?,?,?)
               """       
        webiID = wdoc.getId()
        id = sched.getId()
        name = sched.getName()
        format= sched.getFormat()
        status = sched.getStatus()
        updated = sched.getUpdated()

        values = (webiID,id,name,format,status,updated)
        
        cursor = self.getConnection()
        cursor.execute(sql,values)
        self.getConnection().commit()                
        
    def insertWEBIDataProv(self,wdoc : WEBIDoc,  dprov : WEBIDataProvider):
        sql = f"""insert into WEBIDataProvider ( WEBI_ID ,ID  ,NAME,
            DATA_SOURCE_ID,DATA_SOURCE_TYPE,DATA_SOURCE_LOCATION,
            UPDATED ,ISPARTIAL ,ROWCOUNT ) VALUES (?,?,?,?,?,?,?,?,?)
               """       
        webiID = wdoc.getId()
        id = dprov.getId()
        name = dprov.getName()
        dataSourceId = dprov.getDataSourceId()       
        dataSourceType = dprov.getDataSourceType()
        dataSourceLocation = dprov.getDataSourceLocation()
        updated = dprov.getUpdated()
        isPartial = dprov.getIsPartial()
        rowCount = dprov.getRowCount()
        values = (webiID,id,name,dataSourceId,dataSourceType,dataSourceLocation,
                  updated,isPartial,rowCount)
        cursor = self.getConnection()
        cursor.execute(sql,values)
        self.getConnection().commit()        
        
        
        
    def insertWEBIDoc(self,wdoc : WEBIDoc):
        
        sql = f"""insert into WEBIDocument (ID,CUID,NAME,DESCRIPTION,FOLDER_ID,SCHEDULED,
                    PROPERTIES_FOUND,LASTREFRESH_DATE,MODIFICATION_DATE,DOCUMENT_TYPE,
                    CREATION_DATE,PATH,UPDATED) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
               """
        
        id = wdoc.getId()
        cuid = wdoc.getCUID()
        name = wdoc.getName()
        description = wdoc.getDescription()
        folderId = wdoc.getFolderId()
        scheduled = wdoc.getScheduled()
        propertiesFound = wdoc.getPropertiesFound()
        lastRefreshDate = wdoc.getLastRefreshDate()
        modificationDate = wdoc.getModificationDate()
        documentType = wdoc.getDocumentType()
        creationDate = wdoc.getCreationDate()
        path = wdoc.getPath()
        updated = wdoc.getUpdated()
        values = (id,cuid,name,description,folderId,scheduled,propertiesFound,
                  lastRefreshDate,modificationDate,documentType,creationDate,
                  path,updated)
        
        cursor = self.getConnection()
        cursor.execute(sql,values)
        self.getConnection().commit()
        
    def insertBWQueryBEx(self,qbex : QueryBEx):
        
        sql = f"""insert into BWQueryBEx (COMPID,INFOCUBE,MODTIME,REPTIME) 
                    VALUES (?,?,?,?)
               """
        
        compid = qbex.getCompId()
        infocube = qbex.getInfoCube()
        modtime = qbex.getModTime()
        reptime = qbex.getRepTime()
        
        values = (compid,infocube,modtime,reptime)
        
        cursor = self.getConnection()
        cursor.execute(sql,values)
        self.getConnection().commit()              
    
        
    
    