import os
from pygeotile.tile import Tile
import sys, json, requests, random, string

from amqp_manager import AMQP

import discovery_registration

deviceType  = "video_sensor"

# Parameters from MEC Message Broker
# Configure properly <user>, <password>
username='<user>'
password='<password>'

# Geoposition - Next steps: from GPS device. Now hardcoded.
latitude    = 43.295778
longitude   = -1.980823
deviceCoord = "%s, %s" %(latitude, longitude)       #Lat, Lon

#Server URL to get the MEC information from 5GMETA Cloud Infrastructure
server_url     = ""

sourceType = os.getenv('VIDEO_SOURCE')
sourcePar = os.getenv('VIDEO_PARAM')

if __name__ == '__main__':
    # Getting the tiles from latitude and longitude.
    tileTmp = Tile.for_latitude_longitude(latitude=latitude, longitude=longitude, zoom=25)
    print('(Latitude: %s, Longitude: %s) | QuadTree: %s' %(latitude, longitude, str(tileTmp.quad_tree)))
    tile25 = str(tileTmp.quad_tree)

    tileTmp = Tile.for_latitude_longitude(latitude=latitude, longitude=longitude, zoom=18)
    print('(Latitude: %s, Longitude: %s) | QuadTree: %s' %(latitude, longitude, str(tileTmp.quad_tree)))
    tile18 = str(tileTmp.quad_tree)

    # Replace with your metadata
    dataflowmetadata = {
        "dataTypeInfo": {
            "dataType": "video",
            "dataSubType": "h264"
        },
        "dataInfo": {
            "dataFormat": "webrtc",
            "dataSampleRate": 10.0,
            "dataflowDirection": "upload",
            "extraAttributes": ""
        },
        "licenseInfo": {
            "licenseGeolimit": "europe",
            "licenseType": "profit"
        },
        "dataSourceInfo": {
            "sourceTimezone": 2,
            "sourceStratumLevel": 1,
            "sourceId": 12345678,
            "sourceType": "vehicle",
            "sourceLocationInfo": {
                "locationQuadkey": tile18,
                "locationCountry": "ESP",
                "locationLatitude": latitude,
                "locationLongitude": longitude
            }
        }   
    }

    # Get Message Broker access
    service="message-broker"
    messageBroker_ip, messageBroker_port = discovery_registration.discover_sb_service(tile18,service)
    if messageBroker_ip == -1 or messageBroker_port == -1:
        print(service+" service not found")
        exit(-1)
    
    # Register a New stream into the System
    service="registration-api"
    Registration_ip, Registration_port = discovery_registration.discover_sb_service(tile18,service)
    if Registration_ip == -1 or Registration_port == -1:
        print(service+" service not found")
        exit(-1)

    # Get Video Broker access
    service="video-broker"
    videoBroker_ip, videoBroker_port = discovery_registration.discover_sb_service(tile18,service)
    if videoBroker_ip == -1 or videoBroker_port == -1:
        print(service+" service not found")
        exit(-1)

    # Get Topic and dataFlowId to push data into the Message Broker
    dataflowId, topic, send = discovery_registration.register(dataflowmetadata, tile18)

    print("\n\tREGISTRATION DONE\n")

    server_url="amqp://"+username+":"+password+"@"+messageBroker_ip+":"+str(messageBroker_port)+"/topic://"

    # Prepare all the AMQP parameters to proceed
    parameters = {
        'deviceType' : deviceType, 
        'serverURL' : server_url,
        'vserverIP' : videoBroker_ip,
        'vserverPort' : videoBroker_port,
        'sourceType' : sourceType,
        'sourcePar' : sourcePar,
        'id' : dataflowId, 
        'tile' : tile25, 
        'metadata' : dataflowmetadata,
        'send' : send
    }
    
    topics = ['newdataflow', 'terminatedataflow']
    try:
        source = AMQP(subscription=topics, param=parameters)
    except:
        print('Error running webrtc_proxy')
        raise Exception('Error running webrtc_proxy')
