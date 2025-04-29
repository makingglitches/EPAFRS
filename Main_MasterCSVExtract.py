import csv
import time

import extractkmz as ek

import htmltableextractor
import wgetwrapper


import uuid

import concurrent.futures

import utils as u

import sqlcolmap as sc


checksql = u.readall('sql/check_exists.sql')
insfrssql = u.readall('sql/insert_epafrs.sql')
getprogfields = u.readall('sql/getProgramFields.sql')
insprograms = u.readall('sql/insert_programfields.sql')
getprogramid = u.readall('sql/select_epaprogram.sql')
insepaprogam = u.readall('sql/insert_epaprograms.sql')
getinterestid = u.readall('sql/select_epainterestid.sql')
insepainterest = u.readall('sql/insert_epainterests.sql')

def pad(spaces:int)->str:
    s = ""

    for i in range(spaces+1):
        s=s+" "
    
    return s


def ProcessFRSAsync(facility:dict,url:str, registryid:str, recexists:bool) -> dict:
    html = wgetwrapper.fetch_html_with_wget(url)
    
    tables = None

    if html['returncode'] == 0:
        tables = htmltableextractor.extract_tables_with_bs4(html['stdout'])

    return {
            'error': html['returncode'] !=0,
            'returncode':html['returncode'],
            'facility':facility,
            'url': url,
            'registryid':registryid,
            'frs_table': None if tables is None or len(tables) ==0 else tables[0],
            'programs_table':None if tables is None or len(tables) == 0 else tables[1],
            'recordexists': recexists
            }

def performDatabaseJob(facility:dict, url:str, registryid:str, frs_table:dict, programs_table:dict,recexists:bool)->tuple[int,int]:

    updated = 0
    created = 0

    wconn = u.getSingleModeConnection('output/EPADocker.sqlite',False)
    rconn = u.getSingleModeConnection('output/EPADocker.sqlite',True)

    frs_fields = {}
    program_fields = {}

    frs_table = [] if frs_table is None else frs_table
    
    # pivot data for facilities
    for fac_field in frs_table:
        frs_fields[fac_field['col1'].strip()] = fac_field['col2'].strip()
    
    progheader = True

    programs_table = [] if programs_table is None else programs_table
    # pivot data for programs
    for program_row in programs_table:
        if progheader:
            progheader = False
            continue

        # this updated code is just in case there are multiple same ones.
        key = program_row['col1'].strip()

        if key not in program_fields:
            program_fields[key] = []
        
        interest = program_row['col2'].strip()
        programid = program_row['col3'].strip() if program_row['col3'] else ''

        program_fields[key].append((interest,programid))

    # extract the registry_id from the facility
    # registryid = frs_fields['REGISTRY ID']

    # if record exists doublecheck, otherwise insert it.
    if not recexists:       

        address = None
        county = None
        lat = None
        lon = None
        datum = None
        colmethod = None

        if len(frs_table) > 0:
            address = frs_fields['LOCATION ADDRESS']
            county = frs_fields['COUNTY NAME']
            lat = frs_fields['LATITUDE']
            lon = frs_fields['LONGITUDE']
            datum = frs_fields['HORIZONTAL DATUM']
            colmethod = frs_fields['COLLECTION METHOD']


        # compose the facility record.
        rec = { 'RegistryID':registryid,
        'Title':facility['Title'],
        'Coordinates':facility['Coordinates'],
        'Address':address,
        'City':facility['City'],
        'State':facility['State'],
        'County':county,
        'Link':url,
        'Latitude':lat,
        'Longitude': lon,
        'HDatum': datum,
        'CollectionMethod': colmethod,
        'EmptyLink': len(frs_table) == 0
        }
        # insert the new facility
        wconn.execute(insfrssql,rec)
        created = created + 1
    
    #pull back any existing program entries in case of epa updates preserve
    #all original data.
    programs = sc.getSqlMap(rconn,getprogfields, {'RegistryID':registryid})

    skipping = True

    # iterate through the program fields from the website.
    for program in program_fields:

        i = len(program)-1

        while program[i] != '(':
            i = i -1
        
        programurl = program[i:].strip()
        programname =  program.replace(programurl,'').strip()
        programurl = programurl.replace('(','').replace(')','')
        
        programUUID = rconn.execute(getprogramid,{'Name':programname}).fetchone()
        programUUID =  None if programUUID is None else programUUID[0]

        if programUUID is None:
            programUUID = uuid.uuid4().hex

            newprogrec = {
                    'ProgramId':programUUID,
                    'Name':programname,
                    'URL': programurl
                }
            
            wconn.execute(insepaprogam,newprogrec)
            wconn.commit()
        
        for interest, programid in program_fields[ program]:
            found = False                            
            interestname = interest.strip()
            interestUUID = rconn.execute(getinterestid,{'Name':interestname}).fetchone()                
            interestUUID = None if interestUUID is None else interestUUID[0]

            if interestUUID is None:
                interestUUID = uuid.uuid4().hex
                wconn.execute(insepainterest,{'InterestId':interestUUID, 'Name':interestname})
                wconn.commit()

            # compare against database rows.
            for database_row in programs:                                   
                if database_row['Interest'] == interestUUID and  \
                    database_row['ProgramId'] == programid and \
                    database_row['Program'] == programUUID:
                    # program matches existing data
                    found = True
                    break

            # if program is not in database, add it.
            if not found:        
                progrec = { 'RegistryID':registryid,
                'Program':programUUID,
                'Interest':interestUUID,
                'ProgramId':programid}

                wconn.execute(insprograms,progrec)
                skipping = False

    if not skipping and recexists:
        updated = updated + 1

    wconn.commit()
    wconn.close()

    rconn.close()

    return (created,updated)

def waitAndResubmit(jobs:list[concurrent.futures.Future], 
                    exec:concurrent.futures.ThreadPoolExecutor, 
                    until=0):
     
    updated = 0
    created = 0
    errored = 0
    terror = 0

    cycles = 0
    
    stallmessage = False

    # what this is designed to do is limit the number of concurrently queued jobs
    # which causes execution of job queuing to pause until the number of Future's objects
    # has been reduced to below a specified limit.
    # setting 0 causes it to continue to hold until ALL jobs have finished.
    while len(jobs) > until:

        retjob = []    

        donejobs:list[concurrent.futures.Future] = []

        # scan jobs for finished jobs.
        for job in jobs:
            if job.done():
                donejobs.append(job)
    
        cycles = cycles + 1

        # handle finished jobs, no waiting on running jobs, should accomplish task faster
        for job in donejobs:            
            jobs.remove(job)
            res = job.result()

            if res['error']:
                #print(f'job failed: {res}')
                errored = errored + 1
                terror = terror + (0 if res['returncode'] != 255 else 1)
                retjob.append(res)
            else:
                cr,up = performDatabaseJob(res['facility'], res['url'], res['registryid'], res['frs_table'], res['programs_table'], res['recordexists'])
                updated = updated  + up
                created = created + cr

        if len(donejobs) > 5 or cycles>=20:
        
            if not stallmessage:
                print()
                stallmessage = True
            	            
            print(f"STALLED FOR {0.05*cycles:.3f}s  to:{terror}"+pad(10), end="\r")

        for ret in retjob:
            fut = exec.submit(ProcessFRSAsync,ret['facility'], ret['url'], ret['registryid'],ret['recordexists'])
            jobs.append(fut)

        if len(jobs) > until:
            time.sleep(0.050)
    
    if stallmessage:
        print()
    
    return (jobs, created,updated,errored, terror, cycles*0.05)

def GrabEPAData(csvname, skipExisting=True,jobLimit=12)->tuple[int,int,int]:

    skipped = 0
    updated = 0
    created = 0
    errored = 0

    print(f'Extracting data from epa site for {csvname}')
    
    fields = ["Title", "Coordinates", "State", "City", "Iframe Link"]
    csvfile = open(csvname,'r')
    csvrows = csv.DictReader(csvfile,fields)

   # for single threading put this back.
    rconn = u.getSingleModeConnection('output/EPADocker.sqlite',True)
    
    print()
    print('Fetching Data From EPA..')

    header = True

    count = 0

    exec =  concurrent.futures.ThreadPoolExecutor(jobLimit,'frsworker_')
    jobs:list[concurrent.futures.Future] = []
    

    for facility in csvrows:    
       # for single threading put this back
       # bottleneck is network jobs. 
       # wconn.commit()

        if header:
            header = False
            continue
    
        
        # gather the necessary information for the concurrent job. and the relocated
        # database processing task.

        url = facility['Iframe Link']
        registryid = url[url.index('registry_id')+12:]

        print(f'Processing {registryid}, record {count+1} s: {skipped} c: {created} u: {updated} e: {errored} ', end="\r")   

        count = count + 1

        # check if there is an existing record
        frsrecord = rconn.execute(checksql,{'Link':url}).fetchone()
        
        if skipExisting and frsrecord:
            skipped = skipped + 1
            continue

        # this is where the bottleneck was occuring, the grabbing of frs information
        # with wget was slowing everything.

        waitres = waitAndResubmit(jobs,exec,queueLimit)

        jobs = waitres[0]
        created = created + waitres[1]
        updated = updated + waitres[2]
        errored = errored + waitres[3]

        # append another processing job
        #time.sleep(0.5)
        jobs.append(exec.submit(ProcessFRSAsync, facility,url,registryid,frsrecord is not None ))
    
    # finish up any remaining jobs
    
    waitres = waitAndResubmit(jobs,exec,0)

    jobs = waitres[0]
    created = created + waitres[1]
    updated = updated + waitres[2]
    errored = errored + waitres[3]
  
    rconn.close()
    csvfile.close()

    return (created,updated,skipped)
                        
        
files = []
csvfiles = []

#Set this to true to force comparison of all available data and append
# any new program interest records.
UpdateAll = False

# Set to true if the output files are already in the output/EPA-FRS directory
SkipExtract = True

#Limit the number of concurrent wget connectins
jobLimit = 2

# To allow skipping of files to make the process start faster
startatkmz = 1

# Limit the number of queued items
queueLimit = 100

# create the kmz file list to process, EPA has 10 regions presently.
for i in range(startatkmz,11):
    files.append(f'input/region_{i:02d}.kmz')
                 
# process the compressed kmz, create csv files, add to file csvlist.
for f in files:
    csvfiles.append(  ek.extract_kmz_data(f,'input/',SkipExtract))

# populate database pulling and parsing IFRAME Contents
# updatea as per settings
for c in csvfiles:
    rets = GrabEPAData(c,not UpdateAll, jobLimit)
    print()
    print(f'Created: {rets[0]}  Updated: {rets[1]} Skipped:{rets[2]}')


