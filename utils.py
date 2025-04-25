import psutil
import pathlib
import sqlite3
import urllib.parse
import datetime
import os
import sqlcolmap



def GetErrorCodes(rconn:sqlite3.Connection)->list[dict]:
    errsql = readall('URLExtraction/sql/get_errorcodes.sql')
    codes = sqlcolmap.getSqlMap(rconn,errsql)

    return codes


def recursiveDeleteDirectory(dir:str):
      for d in os.walk(dir):
            for file in d[2]:
                os.remove(os.path.join(dir,file))
                

def countmatch(s:str,match:str)->int:
    
    count = 0
    cont = True
    
    while cont:
        try:
            i = s.index(match)
            count = count + 1
            
        except ValueError:
            cont = False



def popNextMatch(s:str,match:str)->tuple[str,str]:
    """
    Removes the str up until the match string and returns the modified string and the
    extacted string

    Args:
        s (str): a string formatted to be chomped from the front to extract data
        match (str): a seperator character or token

    Returns:
        tuple[str,str]: (remaining string, extracted text with matched string)
    
    Example:
        s = "Santa Clara, CA"
        res = popNextMatch(s,',')
        city = res[1][:-1]
        state = res[0]
    """    
    try:
        ind = s.index(match)
        res = s[:ind]
        withmatch = res + s[ind:ind+len(match)]
        s = s.replace(withmatch,'').strip()
                    
        return (s,res.strip())
    
    except ValueError:

        return (s,None)


def RemoveDoubleSpace(s:str, remnewlines=True)->str:

    if s is None:
        return ''
    
    s = s.replace('\t','')

    if remnewlines:
        s = s.replace('\n', '')

    try:
        while True:
            i = s.index('  ')
            s = s.replace('  ',' ')
    except ValueError:
        return s



def StampedFileName(base:str,ext:str):
    """
    Returns a filename in the form basename.timetamp.ext
    Fuck you people.

    Args:
        base (str): portion of filename before timstamp, no trailing (.)
        ext (str): portion of filename before timestamp, no leading (.)

    Returns:
        str: stamped filename.

    Example:
        StampedFilename('log','txt')
        
        # returns something like  log.1738941931.txt
    """    
    stamp = str(int(datetime.datetime.now(datetime.timezone.utc).timestamp()))
    return f'{base}.{stamp}.{ext}'

def findAll(srcstr:str, findstr:str, case_senstitive:bool=True )->list[int]:

    if srcstr is None or findstr is None:
        raise ValueError("You have to set srcstr and findstr to a value.")
    
    if len(findstr) > len(srcstr):
        return -1

    i = 0

    matches = []

    while len(srcstr) - i >= len(findstr):

        if srcstr[i:len(findstr)] == findstr:
            matches.append(i)
        
        i = i + 1

    return matches


def filepath_to_sqlite_uri(filepath):
    """
    Converts a file path to a SQLite URI format.

    Args:
        filepath (str): The file path to be converted.

    Returns:
        str: The SQLite URI string.
    """
    uri = f"file:{urllib.parse.quote(filepath)}"
    return uri

def getSingleModeConnection(file:str='output/EPADocker.sqlite', readonly:bool=True):
    squri = filepath_to_sqlite_uri(file)+f"?mode={'ro' if readonly else 'rw'}"
    return sqlite3.connect(squri,uri=True)

def correctPath(path:str):

    if path is None:
        return None
    
    p = pathlib.Path(path)
    
    if path.startswith('~'):
        return str(p.expanduser())
    else:
        return str(p.absolute())

def readall(filename:str)->str:
    f = open(filename,"r")
    s =  f.read()
    f.close()
    return s

def getFirstOrNone(alist:list)->any:
    return None if len(alist) == 0 else alist[0]

def IsPartitionMounted(dir):
    for part in psutil.disk_partitions():
        if dir == part.mountpoint:
            return True
    return False

if __name__=="__main__":

   print ('Hi there.')

    