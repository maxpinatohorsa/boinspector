from __future__ import annotations

class QueryBEx:
    
    def __init__(self,compid,infocube,modtime,reptime):
        self._compid = compid 
        self._infocube = infocube
        self._modtime = modtime
        self.__reptime = reptime 
        
    def getCompId(self):
        return self._compid 

    def getInfoCube(self): 
        return self._infocube 
    
    def getModTime(self): 
        return self._modtime
    
    def getRepTime(self):
        return self.__reptime
        