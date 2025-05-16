import sqlalchemy as db
import json


def insert_dataflow_localdb(dataflowmetadata, connection, dataflows, owner, dataflowId):
    # Insert the dataflow in the DB
    query = db.sql.insert(dataflows).values({
        "dataflowId": str(dataflowId),
        "dataType": dataflowmetadata['dataTypeInfo']["dataType"],
        "dataSubType": dataflowmetadata['dataTypeInfo']["dataSubType"],
        "dataFormat": dataflowmetadata['dataInfo']["dataFormat"],
        "dataSampleRate": dataflowmetadata['dataInfo']["dataSampleRate"],
        "licenseGeolimit": dataflowmetadata['licenseInfo']["licenseGeoLimit"],
        "licenseType": dataflowmetadata['licenseInfo']["licenseType"],
        "dataflowAttributes": json.loads(dataflowmetadata['dataInfo']["extraAttributes"].replace("\'", "\"")) if dataflowmetadata['dataInfo']["extraAttributes"] else None,
        "dataflowDirection": dataflowmetadata['dataInfo']["dataFlowDirection"]
    })
    connection.execute(query)

    # Insert the owner in the DB
    query = db.sql.insert(owner).values({
        "ownerId": "1",
        "ownerName": "TestOwner",
    })
    try:
        connection.execute(query)
    except:
        pass

def insert_sensor_local_db(dataflowmetadata, connection, sensor):
    # Insert the sensor in the DB
    query = db.sql.insert(sensor).values({
        "sourceId": dataflowmetadata['dataSourceInfo']["sourceId"],
        "ownerId": "1",
        "sourceName": "Test Vehicle",
        "sourceType": dataflowmetadata['dataSourceInfo']["sourceType"],
        "locationQuadkey": dataflowmetadata['dataSourceInfo']["sourceLocationInfo"]["locationQuadkey"],
        "locationCountry": dataflowmetadata['dataSourceInfo']["sourceLocationInfo"]["locationCountry"],
        "timeZone": dataflowmetadata['dataSourceInfo']["sourceTimezone"],
        "timeStratumLevel": dataflowmetadata['dataSourceInfo']["sourceStratumLevel"],
    })
    try:
        connection.execute(query)
    except:
        pass

def insert_internal_sensor_local_db(connection, internalSensor):
    # Insert the internalSensor in the DB
    query = db.sql.insert(internalSensor).values({
        "internalSensorId": 1,
        "sourceId": "1",
        "internalSensorName": "Sensor 1",
        "internalSensortype": "unknown"
    })
    try:
        connection.execute(query)
    except:
        pass

def insert_dataflow_produced_dataflows_local_db(connection, produced, dataflowId):
    # Insert the dataflow in the producedDataflows in the DB
    query = db.sql.insert(produced).values({
        "producerDataflow": str(dataflowId),
        "producerId": "1",
        "internalProducerId": 1
    })
    connection.execute(query)


def prepare_database(db_user,db_password,db_ip,db_port):
    engine = db.create_engine('mysql+pymysql://'+db_user+':'+db_password+'@'+db_ip+':+'+db_port+'/5GMETA_SD', isolation_level="READ UNCOMMITTED")
    connection = engine.connect()
    metadata = db.MetaData()
    dataflows = db.Table('dataflows', metadata, autoload=True, autoload_with=engine)
    produced = db.Table('producedDataflows', metadata, autoload=True, autoload_with=engine)
    owner = db.Table('sensorOwners', metadata, autoload=True, autoload_with=engine)
    sensor = db.Table('sensorIdentity', metadata, autoload=True, autoload_with=engine)
    internalSensor =  db.Table('internalSensorIdentity', metadata, autoload=True, autoload_with=engine)
    return connection, dataflows,produced,owner,sensor,internalSensor
