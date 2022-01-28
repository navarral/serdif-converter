from pprint import pprint
import requests
import json
import xmltodict
import io
import sys
import pandas as pd
from datetime import datetime
from jinja2 import Environment, FileSystemLoader
from collections import defaultdict
from zipfile import ZipFile


# Function that returns the events filtered by type within specific regions
def evLoc(referer, repo):
    qBody = '''
    PREFIX geo: <http://www.opengis.net/ont/geosparql#>
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
    SELECT ?LOI 
    WHERE { 
        ?county
            a geo:Feature, <http://ontologies.geohive.ie/osi#County> ;
            rdfs:label ?LOI ;
            geo:hasGeometry/geo:asWKT ?countyGeo .
        FILTER (lang(?LOI) = 'en')
    }
    '''
    endpoint = ''.join(referer + repo)
    # 1.3.Fire query and convert results to json (dictionary)
    qEvLoc = requests.post(
        endpoint,
        data={'query': qBody},
        headers={
            'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:93.0) Gecko/20100101 Firefox/93.0',
            'Referer': 'https://serdif-example.adaptcentre.ie/sparql',
            'Accept': 'application/sparql-results+json',
        }
    )
    jEvLoc = json.loads(qEvLoc.text)
    # 1.4.Return results
    rEvLoc = jEvLoc['results']['bindings']
    return rEvLoc


# Function that returns the envo datasets filtered by type within specific regions
def envoDataLoc(referer, repo, envoLoc):
    qBody = '''
    PREFIX qb: <http://purl.org/linked-data/cube#>
    PREFIX geof: <http://www.opengis.net/def/function/geosparql/>
    PREFIX geo: <http://www.opengis.net/ont/geosparql#>
    PREFIX prov: <http://www.w3.org/ns/prov#>
    PREFIX dcat: <http://www.w3.org/ns/dcat#>
    PREFIX dct: <http://purl.org/dc/terms/>
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
    PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
    SELECT ?LOI ?envoDataSet
    WHERE {
        # Filter environmental data within a region
        ?envoDataSet
            a qb:DataSet, geo:Feature, prov:Entity, dcat:Dataset ;
            dct:Location/geo:asWKT ?envoGeo .
        #County geom  
        VALUES ?LOI {''' + ''.join([' "' + envoLocVal + '"@en ' for envoLocVal in envoLoc]) + '''}
        ?county
            a geo:Feature, <http://ontologies.geohive.ie/osi#County> ;
            rdfs:label ?LOI ;
            geo:hasGeometry/geo:asWKT ?countyGeo .
        FILTER(geof:sfWithin(?envoGeo, ?countyGeo))  
    }
    '''
    endpoint = ''.join(referer + repo)
    qEnvoLoc = requests.post(
        endpoint,
        data={'query': qBody},
        headers={
            'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:93.0) Gecko/20100101 Firefox/93.0',
            'Referer': 'https://serdif-example.adaptcentre.ie/sparql',
            'Accept': 'application/sparql-results+json',
        }
    )
    jEnvoLoc = json.loads(qEnvoLoc.text)
    # 1.4.Return results
    rEnvoLoc = jEnvoLoc['results']['bindings']
    return rEnvoLoc


# Function that returns the envo datasets filtered by type within specific regions
def evTimeWindow(referer, repo, evDateT, wLag, wLen):
    qBody = '''
    PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
    SELECT ?evDateT ?dateLag ?dateStart
    WHERE { 
        BIND(xsd:dateTime("''' + str(evDateT) + '''") AS ?evDateT)
        BIND(?evDateT - "P''' + str(wLag) + '''D"^^xsd:duration AS ?dateLag)
        BIND(?dateLag - "P''' + str(wLen) + '''D"^^xsd:duration AS ?dateStart)
    }
    '''
    endpoint = ''.join(referer + repo)
    qEvTW = requests.post(
        endpoint,
        data={'query': qBody},
        headers={
            'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:93.0) Gecko/20100101 Firefox/93.0',
            'Referer': 'https://serdif-example.adaptcentre.ie/sparql',
            'Accept': 'application/sparql-results+json',
        }
    )
    jEvTW = json.loads(qEvTW.text)
    # 1.4.Return results
    rEvTW = jEvTW['results']['bindings']
    return rEvTW


# Function to check envo data is available for at least one event
def evEnvoDataAsk(referer, repo, evEnvoDict):
    # Build block per each event
    qBodyBlockList = []
    for ev in evEnvoDict.keys():
        qBodyBlock = '''
        {
            SELECT DISTINCT ?envoDataSet
            WHERE{
                VALUES ?envoDataSet {''' + ''.join([' <' + envoDS + '> ' for envoDS in evEnvoDict[ev]['envoDataSet']]) + '''}  
                ?obsData
                    a qb:Observation ;
                    qb:dataSet ?envoDataSet ;
                    sdmx-dimension:timePeriod ?obsTime .        
                FILTER(?obsTime > "''' + evEnvoDict[ev]['dateStart'] + '''"^^xsd:dateTime && ?obsTime < "''' + \
                     evEnvoDict[ev]['dateLag'] + '''"^^xsd:dateTime)
            }
        }
        '''
        qBodyBlockList.append(qBodyBlock)

    qBodyBlockUnion = '  UNION  '.join(qBodyBlockList)

    qBody = '''
    PREFIX eg: <http://example.org/ns#>
    PREFIX qb: <http://purl.org/linked-data/cube#>
    PREFIX sdmx-dimension: <http://purl.org/linked-data/sdmx/2009/dimension#>
    PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
    ASK
    WHERE{
    ''' + qBodyBlockUnion + '''   
    }
        '''
    endpoint = ''.join(referer + repo)
    qEvEnvoAsk = requests.post(
        endpoint,
        data={'query': qBody},
        headers={
            'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:93.0) Gecko/20100101 Firefox/93.0',
            'Referer': 'https://serdif-example.adaptcentre.ie/sparql',
            'Accept': 'application/sparql-results+json',
        }
    )
    jEvEnvoAsk = json.loads(qEvEnvoAsk.text)
    # 1.4.Return results
    rEvEnvoAsk = jEvEnvoAsk['boolean']
    return rEvEnvoAsk


# Function to check envo data is available for at least one event
def evEnvoDataSet(referer, repo, evEnvoDict, timeUnit, spAgg):
    # Dictionaries to translate timeUnit to query SPARQL query parameters
    selTimeUnit = {'hour': '?hourT ?dayT ?monthT ?yearT',
                   'day': '?dayT ?monthT ?yearT',
                   'month': '?monthT ?yearT',
                   'year': '?yearT',
                   }
    selTimeUnitRev = {'hour': '?yearT ?monthT ?dayT ?hourT',
                      'day': '?yearT ?monthT ?dayT',
                      'month': '?yearT ?monthT',
                      'year': '?yearT',
                      }

    # Build block per each event
    qBodyBlockList = []
    for ev in evEnvoDict.keys():
        qBodyBlock = '''
        {
            SELECT ?event ''' + selTimeUnitRev[timeUnit] + ''' ?envProp (''' + spAgg + '''(?envVar) AS ?envVar)
            WHERE {
                {
                    SELECT ?obsData ?obsTime
                    WHERE{
                        VALUES ?envoDataSet {''' + ''.join(
            [' <' + envoDS + '> ' for envoDS in evEnvoDict[ev]['envoDataSet']]) + '''}  
                        ?obsData
                            a qb:Observation ;
                            qb:dataSet ?envoDataSet ;
                            sdmx-dimension:timePeriod ?obsTime .        
                        FILTER(?obsTime > "''' + evEnvoDict[ev]['dateStart'] + '''"^^xsd:dateTime && ?obsTime < "''' + \
                     evEnvoDict[ev]['dateLag'] + '''"^^xsd:dateTime)
                    }
                }
                ?obsData ?envProp ?envVar .
                FILTER(datatype(?envVar) = xsd:float)  
                FILTER(?envProp != <http://purl.org/linked-data/sdmx/2009/measure#obsValue>)  
                # String manipulation to aggregate observations per time unit
                BIND(YEAR(?obsTime) AS ?yearT)
                BIND(MONTH(?obsTime) AS ?monthT)
                BIND(DAY(?obsTime) AS ?dayT)
                BIND(HOURS(?obsTime) AS ?hourT)
                BIND("''' + ev.split('/ns#')[1] + '''" AS ?event)
            }
            GROUP BY ?event ?envProp ''' + selTimeUnit[timeUnit] + '''
        }
        '''
        qBodyBlockList.append(qBodyBlock)

    qBodyBlockUnion = '  UNION  '.join(qBodyBlockList)

    qBody = '''
    PREFIX qb: <http://purl.org/linked-data/cube#>
    PREFIX eg: <http://example.org/ns#>
    PREFIX geohive-county-geo: <http://data.geohive.ie/pathpage/geo:hasGeometry/county/>
    PREFIX sdmx-dimension: <http://purl.org/linked-data/sdmx/2009/dimension#>
    PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
    PREFIX geo:	<http://www.opengis.net/ont/geosparql#>
    PREFIX prov: <http://www.w3.org/ns/prov#>
    CONSTRUCT{       
        ?sliceName
            a qb:Slice;
            qb:sliceStructure 			eg:sliceByTime ;
            eg:refArea 				    ?evGeoUri ;
            eg:refEvent        			?eventRef ;
            qb:observation   			?obsName ;
            .

        ?obsName
            a qb:Observation ;
            qb:dataSet 					?datasetName ;
            sdmx-dimension:timePeriod 	?obsTimePeriod ;
            ?envProp 					?envVar ;
            .
    }
    WHERE {
    ''' + qBodyBlockUnion + '''   
        # Fix single digits when using SPARQL temporal functions
        BIND( IF( BOUND(?monthT), IF(STRLEN( STR(?monthT) ) = 2, STR(?monthT), CONCAT("0", STR(?monthT)) ), "01") AS ?monthTF )
        BIND( IF( BOUND(?dayT), IF( STRLEN( STR(?dayT) ) = 2, STR(?dayT), CONCAT("0", STR(?dayT)) ), "01" ) AS ?dayTF )
        BIND( IF( BOUND(?hourT) , IF( STRLEN( STR(?hourT) ) = 2, STR(?hourT), CONCAT("0", STR(?hourT)) ), "00" ) AS ?hourTF )
        # Build dateTime values 
        BIND(CONCAT(str(?yearT),"-",?monthTF,"-",?dayTF,"T",?hourTF,":00:00Z") AS ?obsTimePeriod)
        # Build IRI for the CONSTRUCT
        BIND(IRI(CONCAT("http://example.org/ns#dataset-ee-20211012T120000-IE-QT_", ENCODE_FOR_URI(STR(NOW())))) AS ?datasetName)
        BIND(IRI(CONCAT(STR(?datasetName),"-", ?event ,"-obs-", str(?yearT),?monthTF,?dayTF,"T",?hourTF,"0000Z")) AS ?obsName)
        BIND(IRI(CONCAT(STR(?datasetName),"-", ?event ,"-slice")) AS ?sliceName)
        BIND(IRI(CONCAT(str(?event), "-geo")) AS ?evGeoUri)
        BIND(IRI(CONCAT(STR(?datasetName),"-", ?event ,"-slice")) AS ?sliceName)
        BIND(IRI(CONCAT(STR("http://example.org/ns#"), ?event)) AS ?eventRef)

    }
    '''
    endpoint = ''.join(referer + repo)
    # 1.2.Query parameters
    rQuery = requests.post(
        endpoint,
        data={'query': qBody},
        headers={
            'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:93.0) Gecko/20100101 Firefox/93.0',
            'Referer': 'https://serdif-example.adaptcentre.ie/sparql',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
        }
    )

    return {'queryContent': rQuery.content, 'queryBody': qBody}  # qEvEnvo_dict


# Function that returns the number of events grouped by type
def envoVarNameUnit(referer, repo):
    qBody = '''
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
    PREFIX owl: <http://www.w3.org/2002/07/owl#>
    PREFIX qb: <http://purl.org/linked-data/cube#>
    SELECT ?envoVar ?label ?name ?abb ?info
    WHERE { 
        ?envoVar a owl:DatatypeProperty , qb:MeasureProperty ; 
                 rdfs:label ?label ;
                 rdfs:comment ?name ;
                 unit:abbreviation 	?abb ;
                 rdfs:seeAlso ?info ;
                 .
        FILTER(?label ="o3"@en)
    }
    '''

    endpoint = ''.join(referer + repo)
    qVarNameUnit = requests.post(
        endpoint,
        data={'query': qBody},
        headers={
            'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:93.0) Gecko/20100101 Firefox/93.0',
            'Referer': 'https://serdif-example.adaptcentre.ie/sparql',
            'Accept': 'application/sparql-results+json',
        }
    )
    jVarNameUnit = json.loads(qVarNameUnit.text)
    # 1.4.Return results
    rVarNameUnit = jVarNameUnit['results']['bindings']
    # 1.5.Return results formatted for tooltip_header
    varAbb = [cc['envoVar']['value'].split('http://example.org/ns#has')[1] for cc in rVarNameUnit]
    varDesc = [cc['envoVarName']['value'] for cc in rVarNameUnit]
    tooltipEnvDesc = dict(zip(varAbb, varDesc))

    return tooltipEnvDesc


# Function to convert event-environmental rdf dataset to csv
def eeToCSV(eeRDF, eventDF):
    # Read xml content and convert to dictionary to access the data within
    evEnvoData = json.loads(json.dumps(xmltodict.parse(eeRDF['queryContent'])))
    # Select events
    eventElements = [od['eg:refEvent'] for od in evEnvoData['rdf:RDF']['rdf:Description'] if
                     'eg:refEvent' in od.keys()]
    eventKeys = [d['@rdf:resource'] for d in eventElements if type(d) is dict]
    # Build dictionary with environmental observations associated to events
    ee_dict = dict()
    for ev in eventKeys:
        # Check if there is already an event key available
        ev = ev.split('ns#')[1]
        # print(ev)
        if ev not in ee_dict:
            ee_dict[ev] = {}
            for od in evEnvoData['rdf:RDF']['rdf:Description']:
                if ev + '-obs-' in od['@rdf:about']:
                    dateTimeKey = od['@rdf:about'].split('obs-')[1]
                    # check if there is already an event-dateT pair available
                    if dateTimeKey not in ee_dict[ev]:
                        ee_dict[ev][dateTimeKey] = {}
                    # Store values for specific event-dateTime pair
                    for envProp in od.keys():
                        if 'eg:has' in envProp:
                            envPropKey = envProp.split('eg:has')[1]
                            ee_dict[ev][dateTimeKey][envPropKey] = od[envProp]['#text']

    # Nested dictionary to pandas dataframe
    df_ee = pd.DataFrame.from_dict(
        {(i, j): ee_dict[i][j]
         for i in ee_dict.keys()
         for j in ee_dict[i].keys()},
        orient='index'
    )
    # Multi-index to column
    df_ee = df_ee.reset_index()
    # 1.Convert to CSV
    df_ee_csv = df_ee.to_csv(index=False)
    # 2.ReParse CSV object as text and then read as CSV. This process will
    # format the columns of the data frame to data types instead of objects.
    df_ee_r = pd.read_csv(io.StringIO(df_ee_csv), index_col='level_1').round(decimals=2)
    # Converting the index as dateTime
    df_ee_r.index = pd.to_datetime(df_ee_r.index)
    df_ee_r.rename(columns={'level_0': 'event'}, inplace=True)
    # Sort by event and dateT
    df_ee_r = df_ee_r.rename_axis('dateT').sort_values(by=['event', 'dateT'], ascending=[True, False])
    # df_ee_r.insert(0, df_ee_r.pop(df_ee_r.index('event')))
    # Add lag column as reference
    df_ee_r.reset_index(level=0, inplace=True)
    df_ee_r.insert(loc=2, column='lag', value=df_ee_r.groupby('event')['dateT'].rank('dense', ascending=False))
    # Adjust lag value with the time-window lag
    df_ev = eventDF
    df_ev = df_ev[['event', 'wLag']]
    pd.options.mode.chained_assignment = None
    df_ev['event'] = df_ev['event'].str.replace('http://example.org/ns#', '', regex=True)  # .map(lambda x: x.split('/ns#')[1])
    df_ev.reset_index(drop=True, inplace=True)
    df_ee_r = pd.merge(df_ee_r, df_ev, on='event')
    df_ee_r['lag'] = df_ee_r['lag'] + df_ee_r['wLag'] - 1
    df_ee_r.pop('wLag')
    return df_ee_r


# string to iri
def strToIri(stringL):
    return ['<' + string + '>' for string in stringL]


# selectValueKey and separate list
def selectValueKeyL(df, keyN):
    return df.loc[df['key'] == keyN, 'value'].iloc[0].split(' ')


def selectValueKey(df, keyN):
    return df.loc[df['key'] == keyN, 'value'].iloc[0]


def publicationMetadata(queryTimeStr, timeUnit, spAgg, evEnvoDict, eeVars, evEnvoMetaDf):
    # Load environment for jinja2 templates
    file_loader = FileSystemLoader('templates')
    env = Environment(loader=file_loader)
    # 1. Generate mapping file from template
    # Load eea template file
    metaMap = env.get_template('Envo-Event_publication_template.ttl')
    # Set data dictionary for input
    datasetVersion = '20211221T120000'
    # Edit evEnvoDict input
    dd = defaultdict(list)

    for k in evEnvoDict.keys():
        tt = evEnvoDict[k]
        for key, val in tt.items():
            dd[key].append(val)

    evEnvoDict_e = dict(dd)

    def flatten(d):
        return [i for b in [[i] if not isinstance(i, list) else flatten(i) for i in d] for i in b]

    # Dictionaries to translate timeUnit to query SPARQL query parameters
    selTimeUnit = {'hour': 'HOURS',
                   'day': 'DAYS',
                   'month': 'MONTHS',
                   'year': 'YEARS',
                   }
    selTimeRes = {'hour': 'PT1H',
                  'day': 'P1D',
                  'month': 'P1M',
                  'year': 'P1Y',
                  }

    # Query geometry metadata information
    # 1.3.Fire query and convert results to json (dictionary)
    qEnvInfo = requests.post(
        'https://serdif-example.adaptcentre.ie/repositories/repo-serdif-envo-ie',
        data={'query': '''              
                PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
                PREFIX owl: <http://www.w3.org/2002/07/owl#>
                PREFIX qb: <http://purl.org/linked-data/cube#>
                PREFIX sdmx-measure: <http://purl.org/linked-data/sdmx/2009/measure#>
                PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
                PREFIX unit: <http://qudt.org/vocab/unit/>
                PREFIX eg: <http://example.org/ns#>
                SELECT ?envoVar ?label ?name ?abb (GROUP_CONCAT(?infoS;separator=", ") AS ?infoG)
                WHERE { 
                    VALUES ?envoVar {''' + ' '.join(['eg:has' + eeVar for eeVar in eeVars]) + '''}
                    ?envoVar a owl:DatatypeProperty , qb:MeasureProperty ; 
                             rdfs:label ?label ;
                             rdfs:comment ?name ;
                             unit:abbreviation ?abb ;
                             rdfs:seeAlso ?info ;
                    .
                    BIND(CONCAT("<",STR(?info),">") AS ?infoS)
                }
                GROUP BY ?envoVar ?label ?name ?abb
                '''
              },
        headers={'Accept': 'application/sparql-results+json'}
    ).text
    # 1.4.Return results
    jEnvInfo = json.loads(qEnvInfo)['results']['bindings']
    rEnvInfo_envIRI = ['eg:has' + envI['envoVar']['value'].split('/ns#has')[1] for envI in jEnvInfo]
    rEnvInfo_label = [envI['envoVar']['value'].split('/ns#has')[1] for envI in jEnvInfo]
    rEnvInfo_name = [envI['name']['value'] for envI in jEnvInfo]
    rEnvInfo_abb = ['<' + envI['abb']['value'] + '>' for envI in jEnvInfo]
    rEnvInfo_info = [envI['infoG']['value'] for envI in jEnvInfo]

    metaMap_dict = {
        'version': datasetVersion,
        'queryTime': queryTimeStr,
        'queryDateTime': queryTimeStr[0:4] + '-' + queryTimeStr[4:6] + '-' + queryTimeStr[6:8] + 'T' + queryTimeStr[9:11] + ':' + queryTimeStr[11:13] + ':' + queryTimeStr[13:15] + 'Z',
        'timeUnit': selTimeUnit[timeUnit],
        'aggMethod': spAgg,
        'timeRes': selTimeRes[timeUnit],
        'startDateTime': '2000-01-01T00:00:00Z',
        'endDateTime': '2021-01-01T00:00:00Z',
        'eeVars': rEnvInfo_label,
        'eeVarsD': zip(*[rEnvInfo_name, rEnvInfo_envIRI, rEnvInfo_label,
                         rEnvInfo_abb, rEnvInfo_info]),
        # From metadata csv
        'eventName': selectValueKey(df=evEnvoMetaDf, keyN='eventName'),
        'countryName': selectValueKey(df=evEnvoMetaDf, keyN='countryName'),
        'publisher': ', '.join(strToIri(selectValueKeyL(df=evEnvoMetaDf, keyN='publisher'))),
        'publisherL': strToIri(selectValueKeyL(df=evEnvoMetaDf, keyN='publisher')),
        'license': selectValueKey(df=evEnvoMetaDf, keyN='license'),
        'dataController': ', '.join(strToIri(selectValueKeyL(df=evEnvoMetaDf, keyN='dataController'))),
        'orcid': ', '.join(strToIri(selectValueKeyL(df=evEnvoMetaDf, keyN='orcid'))),
        'orcidL': strToIri(selectValueKeyL(df=evEnvoMetaDf, keyN='orcid')),
        # -- Data Subject -------------------------------------------------------
        'DataSubjectLabel': selectValueKey(df=evEnvoMetaDf, keyN='dataSubjectLabel'),
        'DataSubjectComment': selectValueKey(df=evEnvoMetaDf, keyN='dataSubjectComment'),
        'DataSubjectUrl': selectValueKey(df=evEnvoMetaDf, keyN='dataSubjectUrl'),
        # -- Legal Basis --------------------------------------------------------
        'LegalBasisLabel': selectValueKey(df=evEnvoMetaDf, keyN='legalBasisLabel'),
        'LegalBasisComment': selectValueKey(df=evEnvoMetaDf, keyN='legalBasisComment'),
        'LegalBasisUrl': selectValueKey(df=evEnvoMetaDf, keyN='legalBasisUrl'),
        # -- Personal Data Category ---------------------------------------------
        'PersonalDataCategoryComment': selectValueKey(df=evEnvoMetaDf, keyN='personalDataCategoryComment'),
        'PersonalDataCategoryUrl': selectValueKey(df=evEnvoMetaDf, keyN='personalDataCategoryUrl'),
        # -- ProcessingPurpose --------------------------------------------------
        'ProcessingPurposeComment': selectValueKey(df=evEnvoMetaDf, keyN='processingPurposeComment'),
        'ProcessingPurposeUrl': selectValueKey(df=evEnvoMetaDf, keyN='processingPurposeUrl'),
        # -- Right --------------------------------------------------------------
        'RightClass': ', '.join(selectValueKeyL(df=evEnvoMetaDf, keyN='rightClass')),
        'RightComment': selectValueKey(df=evEnvoMetaDf, keyN='rightComment'),
        'RightUrl': selectValueKey(df=evEnvoMetaDf, keyN='rightUrl'),
        # -- Identification Risk ------------------------------------------------
        'IdentificationRiskComment': selectValueKey(df=evEnvoMetaDf, keyN='identificationRiskComment'),
        # -- Data Set Storage ---------------------------------------------------
        'DataSetStorageStorage': selectValueKey(df=evEnvoMetaDf, keyN='dataSetStorageStorage'),
        'DataSetStorageLocation': selectValueKey(df=evEnvoMetaDf, keyN='dataSetStorageLocation'),
        'DataSetStorageDuration': selectValueKey(df=evEnvoMetaDf, keyN='dataSetStorageDuration'),
        'DataSetStorageComment': selectValueKey(df=evEnvoMetaDf, keyN='dataSetStorageComment'),
        # -- Health Data Access Control -----------------------------------------
        'HealthDataAccessControlComment': selectValueKey(df=evEnvoMetaDf, keyN='healthDataAccessControlComment'),
        # -- Health Data Pseudonymisation ---------------------------------------
        'HealthDataPseudonymisationComment': selectValueKey(df=evEnvoMetaDf, keyN='healthDataPseudonymisationComment'),
        # -- DPIA ---------------------------------------------------------------
        'dpiaComment': selectValueKey(df=evEnvoMetaDf, keyN='dpiaComment'),
        'dpiaUrl': selectValueKey(df=evEnvoMetaDf, keyN='dpiaUrl'),
        # -- Health Data Access Control -----------------------------------------
        'HealthDataAuthorisationComment': selectValueKey(df=evEnvoMetaDf, keyN='healthDataAuthorisationComment'),
        # -- Certification ------------------------------------------------------
        'CertificationComment': selectValueKey(df=evEnvoMetaDf, keyN='certificationComment'),
        # -- Consultation -------------------------------------------------------
        'ConsultationComment': selectValueKey(df=evEnvoMetaDf, keyN='consultationComment'),
        # -- Research Contract --------------------------------------------------
        'ResearchContractDuration': selectValueKey(df=evEnvoMetaDf, keyN='researchContractDuration'),
        'ResearchContractComment': selectValueKey(df=evEnvoMetaDf, keyN='researchContractComment'),
        # -- Research Code Of Conduct -------------------------------------------
        'ResearchCodeOfConductComment': selectValueKey(df=evEnvoMetaDf, keyN='researchCodeOfConductComment'),
        # -- Privacy Notice Comment ---------------------------------------------
        'PrivacyNoticeComment': selectValueKey(df=evEnvoMetaDf, keyN='privacyNoticeComment'),
        # -- Data Policy --------------------------------------------------------
        'DataPolicyComment': selectValueKey(df=evEnvoMetaDf, keyN='dataPolicyComment'),
        # -- Research Risk Management Procedure ---------------------------------
        'ResearchRiskManagementProcedureComment': selectValueKey(df=evEnvoMetaDf, keyN='researchRiskManagementProcedureComment'),
        # -- Research Safeguard -------------------------------------------------
        'ResearchSafeguardComment': selectValueKey(df=evEnvoMetaDf, keyN='researchSafeguardComment'),
        # -- Data Use --------------------------------------------------------
        'DataUseClass': ', '.join(selectValueKeyL(df=evEnvoMetaDf, keyN='dataUseClass')),
        'DataUseComment': selectValueKey(df=evEnvoMetaDf, keyN='dataUseComment'),
    }
    outMap = metaMap.stream(data=metaMap_dict)
    # Export resulting mapping
    fileobj = io.StringIO()
    outMap.dump(fileobj)
    outMapRend = fileobj.getvalue()
    return outMapRend


def publicationMetadataMin(queryTimeStr, timeUnit, spAgg, evEnvoDict, eeVars, evEnvoMetaDf):
    # Load environment for jinja2 templates
    file_loader = FileSystemLoader('templates')
    env = Environment(loader=file_loader)
    # 1. Generate mapping file from template
    # Load eea template file
    metaMap = env.get_template('Envo-Event_publication_min_template.ttl')
    # Set data dictionary for input
    datasetVersion = '20211221T120000'
    # Edit evEnvoDict input
    dd = defaultdict(list)

    for k in evEnvoDict.keys():
        tt = evEnvoDict[k]
        for key, val in tt.items():
            dd[key].append(val)

    evEnvoDict_e = dict(dd)

    def flatten(d):
        return [i for b in [[i] if not isinstance(i, list) else flatten(i) for i in d] for i in b]

    # Dictionaries to translate timeUnit to query SPARQL query parameters
    selTimeUnit = {'hour': 'HOURS',
                   'day': 'DAYS',
                   'month': 'MONTHS',
                   'year': 'YEARS',
                   }
    selTimeRes = {'hour': 'PT1H',
                  'day': 'P1D',
                  'month': 'P1M',
                  'year': 'P1Y',
                  }

    # Query geometry metadata information
    # 1.3.Fire query and convert results to json (dictionary)
    qEnvInfo = requests.post(
        'https://serdif-example.adaptcentre.ie/repositories/repo-serdif-envo-ie',
        data={'query': '''              
                PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
                PREFIX owl: <http://www.w3.org/2002/07/owl#>
                PREFIX qb: <http://purl.org/linked-data/cube#>
                PREFIX sdmx-measure: <http://purl.org/linked-data/sdmx/2009/measure#>
                PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
                PREFIX unit: <http://qudt.org/vocab/unit/>
                PREFIX eg: <http://example.org/ns#>
                SELECT ?envoVar ?label ?name ?abb (GROUP_CONCAT(?infoS;separator=", ") AS ?infoG)
                WHERE { 
                    VALUES ?envoVar {''' + ' '.join(['eg:has' + eeVar for eeVar in eeVars]) + '''}
                    ?envoVar a owl:DatatypeProperty , qb:MeasureProperty ; 
                             rdfs:label ?label ;
                             rdfs:comment ?name ;
                             unit:abbreviation ?abb ;
                             rdfs:seeAlso ?info ;
                    .
                    BIND(CONCAT("<",STR(?info),">") AS ?infoS)
                }
                GROUP BY ?envoVar ?label ?name ?abb
                '''
              },
        headers={'Accept': 'application/sparql-results+json'}
    ).text
    # 1.4.Return results
    jEnvInfo = json.loads(qEnvInfo)['results']['bindings']
    rEnvInfo_envIRI = ['eg:has' + envI['envoVar']['value'].split('/ns#has')[1] for envI in jEnvInfo]
    rEnvInfo_label = [envI['envoVar']['value'].split('/ns#has')[1] for envI in jEnvInfo]
    rEnvInfo_name = [envI['name']['value'] for envI in jEnvInfo]
    rEnvInfo_abb = ['<' + envI['abb']['value'] + '>' for envI in jEnvInfo]
    rEnvInfo_info = [envI['infoG']['value'] for envI in jEnvInfo]

    metaMap_dict = {
        'version': datasetVersion,
        'queryTime': queryTimeStr,
        'queryDateTime': queryTimeStr[0:4] + '-' + queryTimeStr[4:6] + '-' + queryTimeStr[6:8] + 'T' + queryTimeStr[9:11] + ':' + queryTimeStr[11:13] + ':' + queryTimeStr[13:15] + 'Z',
        'timeUnit': selTimeUnit[timeUnit],
        'aggMethod': spAgg,
        'timeRes': selTimeRes[timeUnit],
        'startDateTime': '2000-01-01T00:00:00Z',
        'endDateTime': '2021-01-01T00:00:00Z',
        'eeVars': rEnvInfo_label,
        'eeVarsD': zip(*[rEnvInfo_name, rEnvInfo_envIRI, rEnvInfo_label,
                         rEnvInfo_abb, rEnvInfo_info]),
        # From metadata csv
        'eventName': selectValueKey(df=evEnvoMetaDf, keyN='eventName'),
        'countryName': selectValueKey(df=evEnvoMetaDf, keyN='countryName'),
        'publisher': ', '.join(strToIri(selectValueKeyL(df=evEnvoMetaDf, keyN='publisher'))),
        'publisherL': strToIri(selectValueKeyL(df=evEnvoMetaDf, keyN='publisher')),
        'license': selectValueKey(df=evEnvoMetaDf, keyN='license'),
        'dataController': ', '.join(strToIri(selectValueKeyL(df=evEnvoMetaDf, keyN='dataController'))),
        'orcid': ', '.join(strToIri(selectValueKeyL(df=evEnvoMetaDf, keyN='orcid'))),
        'orcidL': strToIri(selectValueKeyL(df=evEnvoMetaDf, keyN='orcid')),
    }
    outMap = metaMap.stream(data=metaMap_dict)
    # Export resulting mapping
    fileobj = io.StringIO()
    outMap.dump(fileobj)
    outMapRend = fileobj.getvalue()
    return outMapRend


def openreadyMetadata(queryTimeStr, timeUnit, spAgg, qText, evEnvoDict, eeVars, evEnvoMetaDf, fileSize):
    # Load environment for jinja2 templates
    file_loader = FileSystemLoader('templates')
    env = Environment(loader=file_loader)
    # 1. Generate mapping file from template
    # Load eea template file
    metaMap = env.get_template('Envo-Event_openready_template.ttl')
    # Set data dictionary for input
    datasetVersion = '20211221T120000'
    # Edit evEnvoDict input
    dd = defaultdict(list)

    for k in evEnvoDict.keys():
        tt = evEnvoDict[k]
        for key, val in tt.items():
            dd[key].append(val)

    evEnvoDict_e = dict(dd)

    def flatten(d):
        return [i for b in [[i] if not isinstance(i, list) else flatten(i) for i in d] for i in b]

    # Dictionaries to translate timeUnit to query SPARQL query parameters
    selTimeUnit = {'hour': 'HOURS',
                   'day': 'DAYS',
                   'month': 'MONTHS',
                   'year': 'YEARS',
                   }
    selTimeRes = {'hour': 'PT1H',
                  'day': 'P1D',
                  'month': 'P1M',
                  'year': 'P1Y',
                  }

    # Query variable descriptions for metadata information
    # 1.3.Fire query and convert results to json (dictionary)
    qEnvInfo = requests.post(
        'https://serdif-example.adaptcentre.ie/repositories/repo-serdif-envo-ie',
        data={'query': '''              
                PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
                PREFIX owl: <http://www.w3.org/2002/07/owl#>
                PREFIX qb: <http://purl.org/linked-data/cube#>
                PREFIX sdmx-measure: <http://purl.org/linked-data/sdmx/2009/measure#>
                PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
                PREFIX unit: <http://qudt.org/vocab/unit/>
                PREFIX eg: <http://example.org/ns#>
                SELECT ?envoVar ?label ?name ?abb (GROUP_CONCAT(?infoS;separator=", ") AS ?infoG)
                WHERE { 
                    VALUES ?envoVar {''' + ' '.join(['eg:has' + eeVar for eeVar in eeVars]) + '''}
                    ?envoVar a owl:DatatypeProperty , qb:MeasureProperty ; 
                             rdfs:label ?label ;
                             rdfs:comment ?name ;
                             unit:abbreviation ?abb ;
                             rdfs:seeAlso ?info ;
                    .
                    BIND(CONCAT("<",STR(?info),">") AS ?infoS)
                }
                GROUP BY ?envoVar ?label ?name ?abb
                '''
              },
        headers={'Accept': 'application/sparql-results+json'}
    ).text
    # 1.4.Return results
    jEnvInfo = json.loads(qEnvInfo)['results']['bindings']
    rEnvInfo_envIRI = ['eg:has' + envI['envoVar']['value'].split('/ns#has')[1] for envI in jEnvInfo]
    rEnvInfo_label = [envI['envoVar']['value'].split('/ns#has')[1] for envI in jEnvInfo]
    rEnvInfo_name = [envI['name']['value'] for envI in jEnvInfo]
    rEnvInfo_abb = ['<' + envI['abb']['value'] + '>' for envI in jEnvInfo]
    rEnvInfo_info = [envI['infoG']['value'] for envI in jEnvInfo]

    # External data sets used to construct the query
    evRegionsL = [region for region in set(flatten(evEnvoDict_e['region']))]
    # Query geometry metadata information
    # 1.3.Fire query and convert results to json (dictionary)
    qGeoMetadata = requests.post(
        'https://serdif-example.adaptcentre.ie/repositories/repo-serdif-events-ie',
        data={'query': '''
                    PREFIX geo: <http://www.opengis.net/ont/geosparql#>
                    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
                    SELECT *
                    WHERE { 
                        VALUES ?LOI {''' + ''.join([' "' + envoLocVal + '"@en ' for envoLocVal in set(evRegionsL)]) + '''}
                        ?county
                            a geo:Feature, <http://ontologies.geohive.ie/osi#County> ;
                            rdfs:label ?LOI ;
                            geo:hasGeometry/geo:asWKT ?countyGeo .
                    } 
                    '''
              },
        headers={'Accept': 'application/sparql-results+json'}
    ).text
    # 1.4.Return results
    jGeoMetadata = json.loads(qGeoMetadata)['results']['bindings']
    rGeoMetadata_geo = ['<' + geoIRI['county']['value'] + '>' for geoIRI in jGeoMetadata]
    rGeoMetadata_geoLit = ['"""' + geoLit['countyGeo']['value'] + '"""^^geo:wktLiteral' for geoLit in jGeoMetadata]

    # External data sets used to construct the query
    extDataUsed = ['<' + envDS + '>' for envDS in set(flatten(evEnvoDict_e['envoDataSet']))]
    # Extract event information from input dict
    evName = [evIRI.split('http://example.org/ns#')[1] for evIRI in evEnvoDict_e['event']]
    evNum = [evIRI.split('http://example.org/ns#event-')[1] for evIRI in evEnvoDict_e['event']]

    metaMap_dict = {
        'version': datasetVersion,
        'queryTime': queryTimeStr,
        'queryDateTime': queryTimeStr[0:4] + '-' + queryTimeStr[4:6] + '-' + queryTimeStr[6:8] + 'T' + queryTimeStr[9:11] + ':' + queryTimeStr[11:13] + ':' + queryTimeStr[13:15] + 'Z',
        'timeUnit': selTimeUnit[timeUnit],
        'aggMethod': spAgg,
        'timeRes': selTimeRes[timeUnit],
        'startDateTime': min(evEnvoDict_e['dateStart']),
        'endDateTime': max(evEnvoDict_e['dateLag']),
        'eeVars': rEnvInfo_label,
        'eeVarsD': zip(*[rEnvInfo_name, rEnvInfo_envIRI, rEnvInfo_label,
                         rEnvInfo_abb, rEnvInfo_info]),
        # From metadata csv
        'eventName': selectValueKey(df=evEnvoMetaDf, keyN='eventName'),
        'countryName': selectValueKey(df=evEnvoMetaDf, keyN='countryName'),
        'publisher': ', '.join(strToIri(selectValueKeyL(df=evEnvoMetaDf, keyN='publisher'))),
        'publisherL': strToIri(selectValueKeyL(df=evEnvoMetaDf, keyN='publisher')),
        'license': selectValueKey(df=evEnvoMetaDf, keyN='license'),
        'dataController': ', '.join(strToIri(selectValueKeyL(df=evEnvoMetaDf, keyN='dataController'))),
        'orcid': ', '.join(strToIri(selectValueKeyL(df=evEnvoMetaDf, keyN='orcid'))),
        'orcidL': strToIri(selectValueKeyL(df=evEnvoMetaDf, keyN='orcid')),
        # -- Data Subject -------------------------------------------------------
        'DataSubjectLabel': selectValueKey(df=evEnvoMetaDf, keyN='dataSubjectLabel'),
        'DataSubjectComment': selectValueKey(df=evEnvoMetaDf, keyN='dataSubjectComment'),
        'DataSubjectUrl': selectValueKey(df=evEnvoMetaDf, keyN='dataSubjectUrl'),
        # -- Legal Basis --------------------------------------------------------
        'LegalBasisLabel': selectValueKey(df=evEnvoMetaDf, keyN='legalBasisLabel'),
        'LegalBasisComment': selectValueKey(df=evEnvoMetaDf, keyN='legalBasisComment'),
        'LegalBasisUrl': selectValueKey(df=evEnvoMetaDf, keyN='legalBasisUrl'),
        # -- Personal Data Category ---------------------------------------------
        'PersonalDataCategoryComment': selectValueKey(df=evEnvoMetaDf, keyN='personalDataCategoryComment'),
        'PersonalDataCategoryUrl': selectValueKey(df=evEnvoMetaDf, keyN='personalDataCategoryUrl'),
        # -- ProcessingPurpose --------------------------------------------------
        'ProcessingPurposeComment': selectValueKey(df=evEnvoMetaDf, keyN='processingPurposeComment'),
        'ProcessingPurposeUrl': selectValueKey(df=evEnvoMetaDf, keyN='processingPurposeUrl'),
        # -- Right --------------------------------------------------------------
        'RightClass': ', '.join(selectValueKeyL(df=evEnvoMetaDf, keyN='rightClass')),
        'RightComment': selectValueKey(df=evEnvoMetaDf, keyN='rightComment'),
        'RightUrl': selectValueKey(df=evEnvoMetaDf, keyN='rightUrl'),
        # -- Identification Risk ------------------------------------------------
        'IdentificationRiskComment': selectValueKey(df=evEnvoMetaDf, keyN='identificationRiskComment'),
        # -- Data Set Storage ---------------------------------------------------
        'DataSetStorageStorage': selectValueKey(df=evEnvoMetaDf, keyN='dataSetStorageStorage'),
        'DataSetStorageLocation': selectValueKey(df=evEnvoMetaDf, keyN='dataSetStorageLocation'),
        'DataSetStorageDuration': selectValueKey(df=evEnvoMetaDf, keyN='dataSetStorageDuration'),
        'DataSetStorageComment': selectValueKey(df=evEnvoMetaDf, keyN='dataSetStorageComment'),
        # -- Health Data Access Control -----------------------------------------
        'HealthDataAccessControlComment': selectValueKey(df=evEnvoMetaDf, keyN='healthDataAccessControlComment'),
        # -- Health Data Pseudonymisation ---------------------------------------
        'HealthDataPseudonymisationComment': selectValueKey(df=evEnvoMetaDf, keyN='healthDataPseudonymisationComment'),
        # -- DPIA ---------------------------------------------------------------
        'dpiaComment': selectValueKey(df=evEnvoMetaDf, keyN='dpiaComment'),
        'dpiaUrl': selectValueKey(df=evEnvoMetaDf, keyN='dpiaUrl'),
        # -- Health Data Access Control -----------------------------------------
        'HealthDataAuthorisationComment': selectValueKey(df=evEnvoMetaDf, keyN='healthDataAuthorisationComment'),
        # -- Certification ------------------------------------------------------
        'CertificationComment': selectValueKey(df=evEnvoMetaDf, keyN='certificationComment'),
        # -- Consultation -------------------------------------------------------
        'ConsultationComment': selectValueKey(df=evEnvoMetaDf, keyN='consultationComment'),
        # -- Research Contract --------------------------------------------------
        'ResearchContractDuration': selectValueKey(df=evEnvoMetaDf, keyN='researchContractDuration'),
        'ResearchContractComment': selectValueKey(df=evEnvoMetaDf, keyN='researchContractComment'),
        # -- Research Code Of Conduct -------------------------------------------
        'ResearchCodeOfConductComment': selectValueKey(df=evEnvoMetaDf, keyN='researchCodeOfConductComment'),
        # -- Privacy Notice Comment ---------------------------------------------
        'PrivacyNoticeComment': selectValueKey(df=evEnvoMetaDf, keyN='privacyNoticeComment'),
        # -- Data Policy --------------------------------------------------------
        'DataPolicyComment': selectValueKey(df=evEnvoMetaDf, keyN='dataPolicyComment'),
        # -- Research Risk Management Procedure ---------------------------------
        'ResearchRiskManagementProcedureComment': selectValueKey(df=evEnvoMetaDf, keyN='researchRiskManagementProcedureComment'),
        # -- Research Safeguard -------------------------------------------------
        'ResearchSafeguardComment': selectValueKey(df=evEnvoMetaDf, keyN='researchSafeguardComment'),
        # -- Data Use --------------------------------------------------------
        'DataUseClass': ', '.join(selectValueKeyL(df=evEnvoMetaDf, keyN='dataUseClass')),
        'DataUseComment': selectValueKey(df=evEnvoMetaDf, keyN='dataUseComment'),
        # Extra for open ready
        'extDataSetsUsed': ', '.join(set(extDataUsed)),
        'countyGeom': ', '.join(set(rGeoMetadata_geo)),
        'countyGeomGeo': zip(*[set(rGeoMetadata_geo), set(rGeoMetadata_geoLit)]),
        'fileSize': fileSize,
        'queryText': qText,
        # -- Event Description -----------------------------------------------
        'eventDict': zip(*[evName, evNum, set(rGeoMetadata_geo), evName, evName, evEnvoDict_e['evDateT']]),
    }
    outMap = metaMap.stream(data=metaMap_dict)
    # Export resulting mapping
    fileobj = io.StringIO()
    outMap.dump(fileobj)
    outMapRend = fileobj.getvalue()
    return outMapRend


def openreadyMetadataMin(queryTimeStr, timeUnit, spAgg, qText, evEnvoDict, eeVars, evEnvoMetaDf, fileSize):
    # Load environment for jinja2 templates
    file_loader = FileSystemLoader('templates')
    env = Environment(loader=file_loader)
    # 1. Generate mapping file from template
    # Load eea template file
    metaMap = env.get_template('Envo-Event_openready_min_template.ttl')
    # Set data dictionary for input
    datasetVersion = '20211221T120000'
    # Edit evEnvoDict input
    dd = defaultdict(list)

    for k in evEnvoDict.keys():
        tt = evEnvoDict[k]
        for key, val in tt.items():
            dd[key].append(val)

    evEnvoDict_e = dict(dd)

    def flatten(d):
        return [i for b in [[i] if not isinstance(i, list) else flatten(i) for i in d] for i in b]

    # Dictionaries to translate timeUnit to query SPARQL query parameters
    selTimeUnit = {'hour': 'HOURS',
                   'day': 'DAYS',
                   'month': 'MONTHS',
                   'year': 'YEARS',
                   }
    selTimeRes = {'hour': 'PT1H',
                  'day': 'P1D',
                  'month': 'P1M',
                  'year': 'P1Y',
                  }

    # Query variable descriptions for metadata information
    # 1.3.Fire query and convert results to json (dictionary)
    qEnvInfo = requests.post(
        'https://serdif-example.adaptcentre.ie/repositories/repo-serdif-envo-ie',
        data={'query': '''              
                PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
                PREFIX owl: <http://www.w3.org/2002/07/owl#>
                PREFIX qb: <http://purl.org/linked-data/cube#>
                PREFIX sdmx-measure: <http://purl.org/linked-data/sdmx/2009/measure#>
                PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
                PREFIX unit: <http://qudt.org/vocab/unit/>
                PREFIX eg: <http://example.org/ns#>
                SELECT ?envoVar ?label ?name ?abb (GROUP_CONCAT(?infoS;separator=", ") AS ?infoG)
                WHERE { 
                    VALUES ?envoVar {''' + ' '.join(['eg:has' + eeVar for eeVar in eeVars]) + '''}
                    ?envoVar a owl:DatatypeProperty , qb:MeasureProperty ; 
                             rdfs:label ?label ;
                             rdfs:comment ?name ;
                             unit:abbreviation ?abb ;
                             rdfs:seeAlso ?info ;
                    .
                    BIND(CONCAT("<",STR(?info),">") AS ?infoS)
                }
                GROUP BY ?envoVar ?label ?name ?abb
                '''
              },
        headers={'Accept': 'application/sparql-results+json'}
    ).text
    # 1.4.Return results
    jEnvInfo = json.loads(qEnvInfo)['results']['bindings']
    rEnvInfo_envIRI = ['eg:has' + envI['envoVar']['value'].split('/ns#has')[1] for envI in jEnvInfo]
    rEnvInfo_label = [envI['envoVar']['value'].split('/ns#has')[1] for envI in jEnvInfo]
    rEnvInfo_name = [envI['name']['value'] for envI in jEnvInfo]
    rEnvInfo_abb = ['<' + envI['abb']['value'] + '>' for envI in jEnvInfo]
    rEnvInfo_info = [envI['infoG']['value'] for envI in jEnvInfo]

    # External data sets used to construct the query
    evRegionsL = [region for region in set(flatten(evEnvoDict_e['region']))]
    # Query geometry metadata information
    # 1.3.Fire query and convert results to json (dictionary)
    qGeoMetadata = requests.post(
        'https://serdif-example.adaptcentre.ie/repositories/repo-serdif-events-ie',
        data={'query': '''
                    PREFIX geo: <http://www.opengis.net/ont/geosparql#>
                    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
                    SELECT *
                    WHERE { 
                        VALUES ?LOI {''' + ''.join([' "' + envoLocVal + '"@en ' for envoLocVal in set(evRegionsL)]) + '''}
                        ?county
                            a geo:Feature, <http://ontologies.geohive.ie/osi#County> ;
                            rdfs:label ?LOI ;
                            geo:hasGeometry/geo:asWKT ?countyGeo .
                    } 
                    '''
              },
        headers={'Accept': 'application/sparql-results+json'}
    ).text
    # 1.4.Return results
    jGeoMetadata = json.loads(qGeoMetadata)['results']['bindings']
    rGeoMetadata_geo = ['<' + geoIRI['county']['value'] + '>' for geoIRI in jGeoMetadata]
    rGeoMetadata_geoLit = ['"""' + geoLit['countyGeo']['value'] + '"""^^geo:wktLiteral' for geoLit in jGeoMetadata]

    # External data sets used to construct the query
    extDataUsed = ['<' + envDS + '>' for envDS in set(flatten(evEnvoDict_e['envoDataSet']))]
    # Extract event information from input dict
    evName = [evIRI.split('http://example.org/ns#')[1] for evIRI in evEnvoDict_e['event']]
    evNum = [evIRI.split('http://example.org/ns#event-')[1] for evIRI in evEnvoDict_e['event']]

    metaMap_dict = {
        'version': datasetVersion,
        'queryTime': queryTimeStr,
        'queryDateTime': queryTimeStr[0:4] + '-' + queryTimeStr[4:6] + '-' + queryTimeStr[6:8] + 'T' + queryTimeStr[9:11] + ':' + queryTimeStr[11:13] + ':' + queryTimeStr[13:15] + 'Z',
        'timeUnit': selTimeUnit[timeUnit],
        'aggMethod': spAgg,
        'timeRes': selTimeRes[timeUnit],
        'startDateTime': min(evEnvoDict_e['dateStart']),
        'endDateTime': max(evEnvoDict_e['dateLag']),
        'eeVars': rEnvInfo_label,
        'eeVarsD': zip(*[rEnvInfo_name, rEnvInfo_envIRI, rEnvInfo_label,
                         rEnvInfo_abb, rEnvInfo_info]),
        # From metadata csv
        'eventName': selectValueKey(df=evEnvoMetaDf, keyN='eventName'),
        'countryName': selectValueKey(df=evEnvoMetaDf, keyN='countryName'),
        'publisher': ', '.join(strToIri(selectValueKeyL(df=evEnvoMetaDf, keyN='publisher'))),
        'publisherL': strToIri(selectValueKeyL(df=evEnvoMetaDf, keyN='publisher')),
        'license': selectValueKey(df=evEnvoMetaDf, keyN='license'),
        'dataController': ', '.join(strToIri(selectValueKeyL(df=evEnvoMetaDf, keyN='dataController'))),
        'orcid': ', '.join(strToIri(selectValueKeyL(df=evEnvoMetaDf, keyN='orcid'))),
        'orcidL': strToIri(selectValueKeyL(df=evEnvoMetaDf, keyN='orcid')),
        # Extra for open ready
        'extDataSetsUsed': ', '.join(set(extDataUsed)),
        'countyGeom': ', '.join(set(rGeoMetadata_geo)),
        'countyGeomGeo': zip(*[set(rGeoMetadata_geo), set(rGeoMetadata_geoLit)]),
        'fileSize': fileSize,
        'queryText': qText,
        # -- Event Description -----------------------------------------------
        'eventDict': zip(*[evName, evNum, set(rGeoMetadata_geo), evName, evName, evEnvoDict_e['evDateT']]),
    }
    outMap = metaMap.stream(data=metaMap_dict)
    # Export resulting mapping
    fileobj = io.StringIO()
    outMap.dump(fileobj)
    outMapRend = fileobj.getvalue()
    return outMapRend


def serdifAPI(eventDF, metadataDF, timeUnit, spAgg, dataFormat, purpose, metadataType):
    # Format columns for serdif openready
    df = eventDF
    df['event'] = 'http://example.org/ns#event-' + df['event']
    df.index = df['event']
    evEnvoDict = df.transpose().to_dict()
    # Query the data
    print('\nSending query to https://serdif-example.adaptcentre.ie/ ...\n')

    refererVal = 'https://serdif-example.adaptcentre.ie/repositories/',
    repoVal = 'repo-serdif-envo-ie',

    # add additional data to each event: envo data sets to use and event time window
    for ev in evEnvoDict:
        # attach environmental data sets to an event
        envoDataSets = envoDataLoc(
            referer=refererVal,
            repo=repoVal,
            envoLoc=evEnvoDict[ev]['region'],
        )
        envoDataSetList = [envoDS['envoDataSet']['value'] for envoDS in envoDataSets]
        evEnvoDict[ev]['envoDataSet'] = envoDataSetList

        # attach start and lag dates for each event
        evTW = evTimeWindow(
            referer=refererVal,
            repo=repoVal,
            evDateT=evEnvoDict[ev]['evDateT'],
            wLag=evEnvoDict[ev]['wLag'],
            wLen=evEnvoDict[ev]['wLen'],
        )
        evTW_dateLag = [dtlag['dateLag']['value'] for dtlag in evTW]
        evTW_dateStart = [dtst['dateStart']['value'] for dtst in evTW]
        evEnvoDict[ev]['dateLag'] = evTW_dateLag[0]
        evEnvoDict[ev]['dateStart'] = evTW_dateStart[0]

    # check if the envo data is available based on user inputs
    qEnvoAsk = evEnvoDataAsk(
        referer=refererVal,
        repo=repoVal,
        evEnvoDict=evEnvoDict,
    )

    if not qEnvoAsk:
        raise ValueError(
            'No data available for the inputs selected. Please try again with a different region and/or event dates.')

    # query environmental data associated to each event
    qEvEnvoDataSet = evEnvoDataSet(
        referer=refererVal,
        repo=repoVal,
        evEnvoDict=evEnvoDict,
        timeUnit=timeUnit,
        spAgg=spAgg,
    )

    # 4 files will be generated as an open-ready zip:
    # (i) data as csv, (ii) data as rdf, (iii) metadata for publication and (iv) metadata for open-ready as rdf.
    # datetime as string for the files name
    time_str = datetime.now().strftime('%Y%m%dT%H%M%S')
    datasetVersion = '20211221T120000'

    # (i) data as csv
    eeData_csv = eeToCSV(eeRDF=qEvEnvoDataSet, eventDF=eventDF)
    # (ii) data as rdf
    eeData_rdf = qEvEnvoDataSet['queryContent'].decode('utf8')

    # export envo variable names to retrieve the definitions
    eeVarsL = list(eeData_csv)
    notEeVars = ['dateT', 'event', 'lag']
    eeVars = [ele for ele in eeVarsL if ele not in notEeVars]

    if purpose == 'research' and metadataType == 'recommended':
        # (iii) metadata for research as rdf
        eeMetadata_research = openreadyMetadata(
            queryTimeStr=time_str,
            evEnvoDict=evEnvoDict,
            timeUnit=timeUnit,
            spAgg=spAgg,
            evEnvoMetaDf=metadataDF,
            qText=qEvEnvoDataSet['queryBody'],
            fileSize=str(sys.getsizeof(qEvEnvoDataSet['queryContent'])),
            eeVars=list(eeVars),
        )
        if dataFormat == 'datatable':
            with ZipFile('./downloads/' + 'ee-openready-' + datasetVersion + '-QT-' + time_str + '.zip', 'w') as zip_file:
                zip_file.writestr('ee-data-' + datasetVersion + '-QT-' + time_str + '.csv', eeData_csv.to_csv())
                zip_file.writestr('ee-metadata-openready-' + datasetVersion + '-QT-' + time_str + '.ttl',
                                  eeMetadata_research)
        elif dataFormat == 'graph':
            with ZipFile('./downloads/' + 'ee-openready-' + datasetVersion + '-QT-' + time_str + '.zip', 'w') as zip_file:
                zip_file.writestr('ee-data-' + datasetVersion + '-QT-' + time_str + '.rdf', eeData_rdf)
                zip_file.writestr('ee-metadata-openready-' + datasetVersion + '-QT-' + time_str + '.ttl',
                                  eeMetadata_research)
        elif dataFormat == 'both':
            # Write the (i)-(iv) files in a zip
            with ZipFile('./downloads/' + 'ee-openready-' + datasetVersion + '-QT-' + time_str + '.zip', 'w') as zip_file:
                zip_file.writestr('ee-data-' + datasetVersion + '-QT-' + time_str + '.csv', eeData_csv.to_csv())
                zip_file.writestr('ee-data-' + datasetVersion + '-QT-' + time_str + '.rdf', eeData_rdf)
                zip_file.writestr('ee-metadata-openready-' + datasetVersion + '-QT-' + time_str + '.ttl',
                                  eeMetadata_research)

    elif purpose == 'publication' and metadataType == 'recommended':
        # (iv) metadata for publication as rdf
        eeMetadata_publication = publicationMetadata(
            queryTimeStr=time_str,
            evEnvoDict=evEnvoDict,
            timeUnit=timeUnit,
            spAgg=spAgg,
            evEnvoMetaDf=metadataDF,
            eeVars=list(eeVars),
        )

        if dataFormat == 'datatable':
            with ZipFile('./downloads/' +'ee-openready-' + datasetVersion + '-QT-' + time_str + '.zip', 'w') as zip_file:
                zip_file.writestr('ee-data-' + datasetVersion + '-QT-' + time_str + '.csv', eeData_csv.to_csv())
                zip_file.writestr('ee-metadata-publication-' + datasetVersion + '-QT-' + time_str + '.ttl',
                                  eeMetadata_publication)
        elif dataFormat == 'graph':
            with ZipFile('./downloads/' +'ee-openready-' + datasetVersion + '-QT-' + time_str + '.zip', 'w') as zip_file:
                zip_file.writestr('ee-data-' + datasetVersion + '-QT-' + time_str + '.rdf', eeData_rdf)
                zip_file.writestr('ee-metadata-publication-' + datasetVersion + '-QT-' + time_str + '.ttl',
                                  eeMetadata_publication)
        elif dataFormat == 'both':
            # Write the (i)-(iv) files in a zip
            with ZipFile('./downloads/' +'ee-openready-' + datasetVersion + '-QT-' + time_str + '.zip', 'w') as zip_file:
                zip_file.writestr('ee-data-' + datasetVersion + '-QT-' + time_str + '.csv', eeData_csv.to_csv())
                zip_file.writestr('ee-data-' + datasetVersion + '-QT-' + time_str + '.rdf', eeData_rdf)
                zip_file.writestr('ee-metadata-publication-' + datasetVersion + '-QT-' + time_str + '.ttl',
                                  eeMetadata_publication)

    elif purpose == 'research' and metadataType == 'minimum':
        # (iii) metadata for research as rdf
        eeMetadata_research_min = openreadyMetadataMin(
            queryTimeStr=time_str,
            evEnvoDict=evEnvoDict,
            timeUnit=timeUnit,
            spAgg=spAgg,
            evEnvoMetaDf=metadataDF,
            qText=qEvEnvoDataSet['queryBody'],
            fileSize=str(sys.getsizeof(qEvEnvoDataSet['queryContent'])),
            eeVars=list(eeVars),
        )
        if dataFormat == 'datatable':
            with ZipFile('./downloads/' + 'ee-openready-' + datasetVersion + '-QT-' + time_str + '.zip', 'w') as zip_file:
                zip_file.writestr('ee-data-' + datasetVersion + '-QT-' + time_str + '.csv', eeData_csv.to_csv())
                zip_file.writestr('ee-metadata-openready-' + datasetVersion + '-QT-' + time_str + '.ttl',
                                  eeMetadata_research_min)
        elif dataFormat == 'graph':
            with ZipFile('./downloads/' + 'ee-openready-' + datasetVersion + '-QT-' + time_str + '.zip', 'w') as zip_file:
                zip_file.writestr('ee-data-' + datasetVersion + '-QT-' + time_str + '.rdf', eeData_rdf)
                zip_file.writestr('ee-metadata-openready-' + datasetVersion + '-QT-' + time_str + '.ttl',
                                  eeMetadata_research_min)
        elif dataFormat == 'both':
            # Write the (i)-(iv) files in a zip
            with ZipFile('./downloads/' + 'ee-openready-' + datasetVersion + '-QT-' + time_str + '.zip', 'w') as zip_file:
                zip_file.writestr('ee-data-' + datasetVersion + '-QT-' + time_str + '.csv', eeData_csv.to_csv())
                zip_file.writestr('ee-data-' + datasetVersion + '-QT-' + time_str + '.rdf', eeData_rdf)
                zip_file.writestr('ee-metadata-openready-' + datasetVersion + '-QT-' + time_str + '.ttl',
                                  eeMetadata_research_min)

    elif purpose == 'publication' and metadataType == 'minimum':
        # (iv) metadata for publication as rdf
        eeMetadata_publication_min = publicationMetadataMin(
            queryTimeStr=time_str,
            evEnvoDict=evEnvoDict,
            timeUnit=timeUnit,
            spAgg=spAgg,
            evEnvoMetaDf=metadataDF,
            eeVars=list(eeVars),
        )

        if dataFormat == 'datatable':
            with ZipFile('./downloads/' + 'ee-openready-' + datasetVersion + '-QT-' + time_str + '.zip', 'w') as zip_file:
                zip_file.writestr('ee-data-' + datasetVersion + '-QT-' + time_str + '.csv', eeData_csv.to_csv())
                zip_file.writestr('ee-metadata-publication-' + datasetVersion + '-QT-' + time_str + '.ttl',
                                  eeMetadata_publication_min)
        elif dataFormat == 'graph':
            with ZipFile('./downloads/' + 'ee-openready-' + datasetVersion + '-QT-' + time_str + '.zip', 'w') as zip_file:
                zip_file.writestr('ee-data-' + datasetVersion + '-QT-' + time_str + '.rdf', eeData_rdf)
                zip_file.writestr('ee-metadata-publication-' + datasetVersion + '-QT-' + time_str + '.ttl',
                                  eeMetadata_publication_min)
        elif dataFormat == 'both':
            # Write the (i)-(iv) files in a zip
            with ZipFile('./downloads/' + 'ee-openready-' + datasetVersion + '-QT-' + time_str + '.zip', 'w') as zip_file:
                zip_file.writestr('ee-data-' + datasetVersion + '-QT-' + time_str + '.csv', eeData_csv.to_csv())
                zip_file.writestr('ee-data-' + datasetVersion + '-QT-' + time_str + '.rdf', eeData_rdf)
                zip_file.writestr('ee-metadata-publication-' + datasetVersion + '-QT-' + time_str + '.ttl',
                                  eeMetadata_publication_min)

    return 'ee-openready-' + datasetVersion + '-QT-' + time_str + '.zip'


if __name__ == '__main__':
    serdifAPI()
    eeToCSV()
