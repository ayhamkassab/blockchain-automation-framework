#!/usr/bin/python3
import os
import argparse
import shutil
import subprocess
import yaml
import sys
import pprint
#import oyaml as yaml
from collections import OrderedDict
import glob
import numpy as np


class UnsortableList(list):
    def sort(self, *args, **kwargs):
        pass

class UnsortableOrderedDict(OrderedDict):
    def items(self, *args, **kwargs):
        return UnsortableList(OrderedDict.items(self, *args, **kwargs))

yaml.add_representer(UnsortableOrderedDict, yaml.representer.SafeRepresenter.represent_dict)






def genOrdererConfig(domainName, orderersCount):
    config = []
    for ordcounter in range(orderersCount):
        tempConfig = {}
        tempConfig["Name"] = "Orderer{}".format(ordcounter+1)
        tempConfig["Domain"] = "orderer{}.{}".format(ordcounter+1, domainName)
        tempConfig["Specs"] = {"- Hostname": 'fabric-ord{}'.format(ordcounter+1)}

        config.append(tempConfig)
    #config.append({
    #    "Name": "Orderer",
    #    "Domain": domainName,
    #    "Specs": [{"Hostname": "orderer{}".format(e)} for e in range(orderersCount)],
    #})

    return config


def getHosteNames(org, peerCounts):
    config = []
    for x in range(peerCounts[org]):
        config.append('Hostname : fabric-peer{}'.format(x+1))
    #print config
    return config


def genPeerConfig(domainName, orgsCount, peerCounts):
    config = []

    for org in range(orgsCount):
        tempConfig = {
            "Name": "Org{}".format(org+1),
            "Domain": "org{}.{}".format(org+1, domainName),
            "Users": {"Count": 0},
            #"Template" : {
            #    "Count": peerCounts[org],
            #    "Hostname": 'fabric-peer1'},#"Hostname": '{{{{ include "fabric.name" . }}}}-peer{}'.format(org+1)#the hostname in template is only duplicating the first hostname in specs to overwrite eachother
            "Specs" : getHosteNames(org, peerCounts)
        }

        config.append(tempConfig)

    return config



"""
def NewgenOrdererConfig(domainName, orderersCount):
    config = []

    for ordcounter in range(orderersCount):
        tempConfig = {}

        tempConfig["Name"] = "Orderer{}".format(ordcounter+1)
        tempConfig["Domain"] = "orderer{}.{}".format(ordcounter+1, domainName)
        tempConfig["Specs"] = {"Hostname": 'fabric-ord{}'.format(ordcounter+1)}

        config.append(tempConfig)
    print(tempConfig,type(tempConfig),type(tempConfig["Name"]),type(tempConfig["Domain"]),type(tempConfig["Specs"]))
    print("************************tempConfig")

    pprint.pprint(tempConfig)
    return config


def NewgenPeerConfig(domainName, orgsCount, peerCounts):
    config = []

    for org in range(orgsCount):
        nodeConfig = {
            "Hostname": 'fabric-peer{}'.format(org+1),
            "Count": peerCounts[org]

        }
        tempConfig = {
            "Name": "Org{}".format(org+1),
            "Domain": "org{}.{}".format(org+1, domainName),
            "Template": nodeConfig,
            "EnableNodeOUs": True,
            "Users" : {
                "Count": peerCounts[org]
            },
        }

        config.append(tempConfig)

    return config
"""
def genCrypto(domainName, orgsCount, orderersCount, peerCounts):
    config = {}

    config["OrdererOrgs"] = genOrdererConfig(domainName, orderersCount)
    config["PeerOrgs"] = genPeerConfig(domainName, orgsCount, peerCounts)

    #print(config)

    fHandle = open("crypto-config.yaml", "w")
    #d = UnsortableOrderedDict(config)
    stream = yaml.dump(config, default_flow_style = False, sort_keys=True)
    #stream2 = stream.replace("'", "")
    fHandle.write(stream.replace("'", ""))



    #fHandle.write(yaml.dump(config, default_flow_style=False))
    fHandle.close()

def genZookeeperService(imageName, networkName, domainName, zooKeepersCount, index):
    serviceName = "zookeeper{}".format(index)
    serviceConfig = {
        "hostname": "zookeeper{}.{}".format(index, domainName),
        "image": imageName,
        "networks": {
            networkName: {
                "aliases": [
                    "zookeeper{}.{}".format(index, domainName),
                ],
            }
        },
        "environment": [
            "CORE_VM_DOCKER_HOSTCONFIG_NETWORKMODE={}".format(networkName),
            "ZOO_MY_ID={}".format(index+1),
            "ZOO_SERVERS={}".format(" ".join(["server.{}=zookeeper{}:2888:3888".format(e+1, e) for e in range(zooKeepersCount)]))
        ],
    }

    return { serviceName : serviceConfig }

def genKafkaService(imageName, networkName, domainName, zooKeepersCount, index):
    serviceName = "kafka{}".format(index)
    serviceConfig = {
        "hostname": "kafka{}.{}".format(index, domainName),
        "image": imageName,
        "networks": {
            networkName: {
                "aliases": [
                    "kafka{}.{}".format(index, domainName),
                ],
            }
        },
        "environment": [
            "CORE_VM_DOCKER_HOSTCONFIG_NETWORKMODE={}".format(domainName),
            "KAFKA_MESSAGE_MAX_BYTES={}".format(15728640),
            "KAFKA_REPLICA_FETCH_MAX_BYTES={}".format(15728640),
            "KAFKA_UNCLEAN_LEADER_ELECTION_ENABLE={}".format(False),
            "KAFKA_DEFAULT_REPLICATION_FACTOR={}".format(3),
            "KAFKA_MIN_INSYNC_REPLICAS={}".format(2),
            "KAFKA_ZOOKEEPER_CONNECT={}".format(" ".join(["zookeeper{}.{}:2181".format(e, domainName) for e in range(zooKeepersCount)])),
            "KAFKA_BROKER_ID={}".format(index),
            "KAFKA_LOG_RETENTIONMS={}".format(-1),
        ],
    }

    return { serviceName : serviceConfig }


def genOrdererService(imageName, networkName, domainName, loggingLevel, index, kafka=False):
    serviceName = "orderer{}".format(index)
    serviceConfig = {
        "hostname": "orderer{}.{}".format(index, domainName),
        "image": imageName,
        "environment": [
            "ORDERER_GENERAL_LOGLEVEL={}".format(loggingLevel),
            "ORDERER_GENERAL_LISTENADDRESS=0.0.0.0",
            "ORDERER_GENERAL_GENESISMETHOD=file",
            "ORDERER_GENERAL_GENESISFILE=/var/hyperledger/orderer/orderer.genesis.block",
            "ORDERER_GENERAL_LOCALMSPID=OrdererMSP",
            "ORDERER_GENERAL_LOCALMSPDIR=/var/hyperledger/orderer/msp",
            "ORDERER_GENERAL_TLS_ENABLED=true",
            "ORDERER_GENERAL_TLS_PRIVATEKEY=/var/hyperledger/orderer/tls/server.key",
            "ORDERER_GENERAL_TLS_CERTIFICATE=/var/hyperledger/orderer/tls/server.crt",
            "ORDERER_GENERAL_TLS_ROOTCAS=[/var/hyperledger/orderer/tls/ca.crt]",
        ],
        "working_dir": "/opt/gopath/src/github.com/hyperledger/fabric",
        "command": "orderer",
        "volumes": [
            "/shared/channel-artifacts/genesis.block:/var/hyperledger/orderer/orderer.genesis.block",
            "/shared/crypto-config/ordererOrganizations/example.com/orderers/orderer0.example.com/msp:/var/hyperledger/orderer/msp",
            "/shared/crypto-config/ordererOrganizations/example.com/orderers/orderer0.example.com/tls/:/var/hyperledger/orderer/tls",
        ],
        "networks": {
            networkName: {
                "aliases": [
                    "orderer{}.{}".format(index, domainName)
                ],
            }
        }
    }
    if kafka:
        serviceConfig["environment"].append("ORDERER_KAFKA_RETRY_SHORTINTERVAL=1s")
        serviceConfig["environment"].append("ORDERER_KAFKA_RETRY_SHORTTOTAL=30s")
        serviceConfig["environment"].append("ORDERER_KAFKA_VERBOSE=true")

    return { serviceName : serviceConfig }

def genPeerService(imageName, networkName, domainName, orgIndex, peerIndex, loggingLevel):
    serviceName = "peer{}_org{}".format(peerIndex, orgIndex)
    serviceConfig = {
        "hostname": "peer{}.org{}.{}".format(peerIndex, orgIndex, domainName),
        "image": imageName,
        "environment": [
            "CORE_PEER_ID=peer{}.org{}.{}".format(peerIndex, orgIndex, domainName),
            "CORE_PEER_ADDRESS=peer{}.org{}.{}:7051".format(peerIndex, orgIndex, domainName),
            "CORE_PEER_GOSSIP_BOOTSTRAP=peer{}.org{}.{}:7051".format(0 if peerIndex!=0 else 1, orgIndex, domainName),
            "CORE_PEER_GOSSIP_EXTERNALENDPOINT=peer{}.org{}.{}:7051".format(peerIndex, orgIndex, domainName),
            "CORE_PEER_LOCALMSPID=Org{}MSP".format(orgIndex),
            "CORE_VM_ENDPOINT=unix:///host/var/run/docker.sock",
            "CORE_VM_DOCKER_HOSTCONFIG_NETWORKMODE=hyperledger-ov",
            "CORE_LOGGING_LEVEL={}".format(loggingLevel),
            "CORE_PEER_TLS_ENABLED=true",
            "CORE_PEER_GOSSIP_USELEADERELECTION=true",
            "CORE_PEER_GOSSIP_ORGLEADER=false",
            "CORE_PEER_PROFILE_ENABLED=true",
            "CORE_PEER_TLS_CERT_FILE=/etc/hyperledger/fabric/tls/server.crt",
            "CORE_PEER_TLS_KEY_FILE=/etc/hyperledger/fabric/tls/server.key",
            "CORE_PEER_TLS_ROOTCERT_FILE=/etc/hyperledger/fabric/tls/ca.crt",
        ],
        "working_dir": "/opt/gopath/src/github.com/hyperledger/fabric/peer",
        "command": "peer node start",
        "deploy": {
            "resources": {
                "reservations": {
                    "cpus": "1",
                    "memory": "1g",
                }
            }
        },
        "volumes": [
            "/var/run/:/host/var/run/",
            "/shared/crypto-config/peerOrganizations/org{org}.{domain}/peers/peer{peer}.org{org}.{domain}/msp:/etc/hyperledger/fabric/msp".format(peer=peerIndex, org=orgIndex, domain=domainName),
            "/shared/crypto-config/peerOrganizations/org{org}.{domain}/peers/peer{peer}.org{org}.{domain}/tls:/etc/hyperledger/fabric/tls".format(peer=peerIndex, org=orgIndex, domain=domainName)
        ],
        "networks": {
            networkName: {
                "aliases": [
                    "peer{}.org{}.{}".format(peerIndex, orgIndex, domainName),
                ],
            }
        },
    }

    return { serviceName : serviceConfig }

def genCliService(imageName, networkName, domainName, loggingLevel):
    serviceName = "cli"
    serviceConfig = {
        "image": imageName,
        "environment": [
              "GOPATH=/opt/gopath",
              "CORE_VM_ENDPOINT=unix:///host/var/run/docker.sock",
              "CORE_LOGGING_LEVEL={}".format(loggingLevel),
              "CORE_PEER_ID=cli",
              "CORE_PEER_ADDRESS=peer0.org1.example.com:7051",
              "CORE_PEER_LOCALMSPID=Org1MSP",
              "CORE_PEER_TLS_ENABLED=true",
              "CORE_PEER_TLS_CERT_FILE=/opt/gopath/src/github.com/hyperledger/fabric/peer/crypto/peerOrganizations/org1.example.com/peers/peer0.org1.example.com/tls/server.crt",
              "CORE_PEER_TLS_KEY_FILE=/opt/gopath/src/github.com/hyperledger/fabric/peer/crypto/peerOrganizations/org1.example.com/peers/peer0.org1.example.com/tls/server.key",
              "CORE_PEER_TLS_ROOTCERT_FILE=/opt/gopath/src/github.com/hyperledger/fabric/peer/crypto/peerOrganizations/org1.example.com/peers/peer0.org1.example.com/tls/ca.crt",
              "CORE_PEER_MSPCONFIGPATH=/opt/gopath/src/github.com/hyperledger/fabric/peer/crypto/peerOrganizations/org1.example.com/users/Admin@org1.example.com/msp",
        ],
        "working_dir": "/opt/gopath/src/github.com/hyperledger/fabric/peer",
        "command": "sleep 1d",
        "volumes": [
            "/var/run/:/host/var/run/",
            "/shared/chaincode/:/opt/gopath/src/github.com/hyperledger/fabric/examples/chaincode/go",
            "/shared/crypto-config:/opt/gopath/src/github.com/hyperledger/fabric/peer/crypto/",
            "/shared/scripts:/opt/gopath/src/github.com/hyperledger/fabric/peer/scripts/",
            "/shared/channel-artifacts:/opt/gopath/src/github.com/hyperledger/fabric/peer/channel-artifacts",
        ],
        "networks": {
            networkName: {
                "aliases": [
                    "cli",
                ],
            }
        },
    }

    return { serviceName : serviceConfig }

def generateDocker(repoOwner, networkName, domainName, orgsCount, orderersCount, peerCounts, zooKeepersCount, kafkasCount, loggingLevel):
    config = {
        "version": '3',
        "networks": {
            networkName: {
                "external": True,
            },
        },
        "services": {}
    }

    for orderer in range(orderersCount):
        config["services"].update(genOrdererService("{}/fabric-orderer:latest".format(repoOwner), networkName, domainName, loggingLevel, orderer, kafkasCount>0))
    for org in range(orgsCount):
        for peer in range(peerCounts[org]):
            config["services"].update(genPeerService("berendeanicolae/fabric-peer:latest".format(repoOwner), networkName, domainName, org+1, peer, loggingLevel))
    for zooKeeper in range(zooKeepersCount):
        config["services"].update(genZookeeperService("{}/fabric-zookeeper:latest".format(repoOwner), networkName, domainName, zooKeepersCount, zooKeeper))
    for kafka in range(kafkasCount):
        config["services"].update(genKafkaService("{}/fabric-kafka:latest".format(repoOwner), networkName, domainName, zooKeepersCount, kafka))
    config["services"].update(genCliService("berendeanicolae/fabric-tools:latest".format(repoOwner), networkName, domainName, loggingLevel))

    fHandle = open("docker-compose-cli.yaml", "w")
    fHandle.write(yaml.dump(config, default_flow_style=False))
    fHandle.close()

def genNetworkOrgs(domainName, orgsCount, orderersCount, peerCounts):
    config = []
    for ordcounter in range(orderersCount):
        ordererConfig = {}
        ordererConfig["Name"] = "Orderer{}Org".format(ordcounter+1)
        ordererConfig["ID"] = "Orderer{}MSP".format(ordcounter+1)
        ordererConfig["MSPDir"] = "./crypto-config/ordererOrganizations/orderer{}.svc.cluster.local/msp".format(ordcounter+1)
        ordererConfig["AdminPrincipal"] = "Role.MEMBER"

        config.append(ordererConfig)


    for org in range(orgsCount):
        orgConfig = {}
        orgConfig["Name"] = "Org{}MSP".format(org+1)
        orgConfig["ID"] = "Org{}MSP".format(org+1)
        orgConfig["MSPDir"] = "crypto-config/peerOrganizations/org{}.svc.cluster.local/msp".format(org+1)
        orgConfig["AdminPrincipal"] = "Role.MEMBER"
        orgConfig["AnchorPeers"] = []
        for peerPerOrg in range(peerCounts[org]):
            orgConfig["AnchorPeers"].append({
                "Host": "fabric-peer{}org{}".format(peerPerOrg+1, org+1),
                "Port": 7051,
            })
        config.append(orgConfig)

    return config




def genNetworkCapabilities():
    config = {}
    config["Global"] = {"V1_1": True}
    config["Orderer"] = {"V1_1": True}
    config["Application"] = {"V1_2": True}

    return config
def genNetworkApplication():
    config = {}

    config["Organizations"] = None

    return config



def genNetworkOrderer(domainName, orderersCount):
    config = {}
    config["OrdererType"] = "kafka"
    config["Addresses"] = ['fabric-ord{}:7050'.format(ordCount+1, domainName) for ordCount in range(orderersCount)]
    config["BatchTimeout"] = "2s"
    config["BatchSize"] = {
        "MaxMessageCount": 10,
        "AbsoluteMaxBytes": 10485760,
        "PreferredMaxBytes": 524288,
    }
    config["Kafka"] = {
        "Brokers": ['fabric-kafka-hlf:9092']
    }
    config["Organizations"] = None

    #if kafkasCount>0:
    #    config["OrdererType"] = "kafka"
    #    config["Kafka"]["Brokers"] = ["kafka{}.{}".format(e, domainName) for e in range(kafkasCount)]

    return config


def setNetworkProfiles(config, domainName):
    config["Profiles"] = {}

    config["Profiles"]["OrdererGenesis"] = {
        "Capabilities": {
            "V1_1": True,
        },
        "Orderer": {
            "OrdererType": config["Orderer"]["OrdererType"],
            "Addresses": config["Orderer"]["Addresses"],
            "BatchTimeout": config["Orderer"]["BatchTimeout"],
            "BatchSize": config["Orderer"]["BatchSize"],
            "Kafka": config["Orderer"]["Kafka"],
            "Organizations": [org for org in config["Organizations"] if "Orderer" in org["ID"]],
            "Capabilities": {
                "V1_1": True
            }
        },
        "Consortiums": {
            "MyConsortium": {
                "Organizations": [org for org in config["Organizations"] if "Org" in org["ID"]]
            },
        },
    }
    config["Profiles"]["MyChannel"] = {
        "Consortium": "MyConsortium",
        "Application": {
            "Organizations": [org for org in config["Organizations"] if "Org" in org["ID"]],
            "Capabilities": {
                "V1_2": True,
            }
        }
    }

def genNetwork(domainName, orgsCount, orderersCount, peerCounts):
    config = {}

    config["Organizations"] = genNetworkOrgs(domainName, orgsCount, orderersCount, peerCounts)
    config["Capabilities"] = genNetworkCapabilities()
    config["Application"] = genNetworkApplication()
    config["Orderer"] = genNetworkOrderer(domainName, orderersCount)
    setNetworkProfiles(config, domainName)

    fHandle = open("configtx.yaml", "w")
    #fHandle.write(yaml.dump(config, default_flow_style=False))
    stream = yaml.dump(config, default_flow_style = False, sort_keys=True)
    fHandle.write(stream.replace("'", ""))
    fHandle.close()




    #config["BatchTimeout"] = "2s"
    #config["BatchSize"] = {
    #    "MaxMessageCount": 10,
    #    "AbsoluteMaxBytes": 10485760,
    #    "PreferredMaxBytes": 524288,
    #}
    #config["Kafka"] = {
    #    "Brokers": []
    #}
    #config["Organizations"] = None


def NewgetKafkaBrokers(domainName, kafkasCount):
    tempKafka= []
    if kafkasCount>0:
        for e in range(kafkasCount):
            tempKafka.append("kafka{}-hlf.{}:9092".format(e, domainName))
    else:
        tempKafka.append("kafka-hlf.orderers.svc.cluster.local:9092")

    return tempKafka

def NewsetNetworkProfiles(config, domainName):
    config["Profiles"] = {}

    config["Profiles"]["OrdererGenesis"] = {

        "Orderer": {
            '<<' : '*OrdererDefaults',
            "Organizations": [org for org in config["Organizations"] if "Orderer" in org["ID"]],
        },
        "Consortiums": {
            "MyConsortium": {
                "Organizations": [org for org in config["Organizations"] if "Org" in org["ID"]]
            },
        },
    }
    config["Profiles"]["MyChannel"] = {
        "Consortium": "MyConsortium",
        "Application": {
            '<<' : '*ApplicationDefaults',
            "Organizations": [org for org in config["Organizations"] if "Org" in org["ID"]],
        }
    }

def NewgenNetwork(domainName, orgsCount, orderersCount, kafkasCount):
    config = {}

    config["Organizations"] = genNetworkOrgs(domainName, orgsCount, orderersCount)
    config["Application"] = '&ApplicationDefaults'

    #config["Application"]["Organizations"] = None
    config["Orderer"] = {"OrdererType" : "kafka","Addresses" : ['fabric-ord{}:7050'.format(e, domainName) for e in range(orderersCount)], "BatchSize" : {
        "MaxMessageCount": 10,
        "AbsoluteMaxBytes": 10485760,
        "PreferredMaxBytes": 524288,
    }, "Kafka" : {'Brokers' : ['{{ include "fabric.name" . }}-kafka-hlf:9092'] , "Organizations": None}}


    #config["Profiles"] = setNetworkProfiles(config, domainName, orgsCount, orderersCount)
    setNetworkProfiles(config, domainName)

    fHandle = open("configtx.yaml", "w")
    stream = yaml.dump(config, default_flow_style = False)
    #stream2 = stream.replace("'<<'", "<<")
    stream2 = stream.replace("null", "")
    #stream4 = stream3.replace("'&ApplicationDefaults'", "&ApplicationDefaults")
    #stream5 = stream4.replace("'*OrdererDefaults'", "*OrdererDefaults")
    #fHandle.write(stream5.replace("'*ApplicationDefaults'", "*ApplicationDefaults"))
    fHandle.write(stream2.replace("'", ""))
    #fHandle.write(yaml.dump(config, default_flow_style=False))
    fHandle.close()



def getKeyFilesInFolder(org, peerPerOrg, domainName):
    print "crypto-config/peerOrganizations/org{}.{}/peers/fabric-peer{}-org{}.{}/msp/keystore/*".format(org+1, domainName, peerPerOrg+1, org+1,domainName)
    return glob.glob("crypto-config/peerOrganizations/org{}.{}/peers/fabric-peer{}-org{}.{}/msp/keystore/*".format(org+1, domainName, peerPerOrg+1, org+1,domainName))

def genCaliperClients(domainName, orgsCount, orderersCount, peerCounts):
    clConfig = {}
    for org in range(orgsCount):
        for peerPerOrg in range(peerCounts[org]):
            clConfig["client{}.org{}.{}".format(peerPerOrg+1, org+1, domainName)] = {
                 "client" : {
                    "organization" : "Org{}".format(org+1),
                    "credentialStore" : {
                        "path" : "/tmp/hfc-kvs/org{}".format(org+1),
                        "cryptoStore" : {
                            "path" : "/tmp/hfc-kvs/org{}".format(org+1),}},
                        #"affiliation": "aff{}".format(peerPerOrg+1)}}
                    "clientPrivateKey" : {
                        "path" : "crypto-config/peerOrganizations/org{}.{}/peers/fabric-peer{}.org{}.{}/msp/keystore/key.pem".format(org+1, domainName, peerPerOrg+1, org+1,domainName)},
                    "clientSignedCert" : {
                        "path" : "crypto-config/peerOrganizations/org{}.{}/peers/fabric-peer{}.org{}.{}/msp/signcerts/fabric-peer{}.org{}.{}-cert.pem".format(org+1, domainName, peerPerOrg+1, org+1, domainName, peerPerOrg+1,org+1,domainName)}}}
            #clConfig["client{}.org{}.{}".format(peerPerOrg+1, org+1, domainName)]["client"]["clientSignedCert"]["path"] = getKeyFilesInFolder(org, peerPerOrg, domainName)

    return clConfig





def ordListInsideChannels(orderersCount):
    config = []
    for ordcounter in range(orderersCount):
        config.append("ord{}-hlf-ord".format(ordcounter+1))

    return config


def peerListInsideChannels(orgsCount, peerCounts):
    clConfig = {}
    for org in range(orgsCount):
        for peerPerOrg in range(peerCounts[org]):
            clConfig["fabric-peer{}org{}".format(peerPerOrg+1, org+1)] = {"eventSource" : "true"}

    return clConfig



def MSPList(orgNames):
    mspConfig=''
    for x in range(len(orgNames)):
        if x !=0:
            mspConfig = mspConfig + ', "{}MSP"'.format(orgNames[x])
        else:
            mspConfig = mspConfig + '"{}MSP"'.format(orgNames[x])

        #mspConfig.append('{}MSP'.format(x))
    return '[{}]'.format(mspConfig)

def contractsIdentitiesList(orgNames):
    mspConfig=[]
    for x in orgNames:
        clConfig = {'role': {'name': 'member', 'mspId': '{}MSP'.format(x)}}
        mspConfig.append(clConfig)
    return mspConfig


#'create': {'buildTransaction': {'version': 0, 'consortium': 'SupplyChainConsortium', 'capabilities': [], 'msps': ['carrierMSP', 'manufacturerMSP']}}, 'channelName': 'allchannel'}]}



def genCaliperChannels(domainName, orgsCount, orderersCount, chaincodeName, peerCounts, chaincodeversion, chaincode_lang, chaincode_init_function, chaincode_path, orgNames, chaincode_init_arguments, chaincode_created):
    chConfig = {}
    tempList=[]
    chConfig["channelName"] = "allchannel"
    chConfig["create"] = {'buildTransaction': {'version': 0, 'consortium': '"SupplyChainConsortium"', 'capabilities': []}}
    chConfig["create"]["buildTransaction"]["msps"] = MSPList(orgNames)
    #chConfig["contracts"] = contractsListInsideChannels(orgsCount, peerCounts)

    #tempList.append({"id" : chaincodeName,
    #            "contractID" : chaincodeName,
    #           "install": {
    #                "version" : chaincodeversion,
    #                 "language" : chaincode_lang,
    #                "path" : chaincode_path,},
    #           'instantiate': {'initFunction': chaincode_init_function},
    #           "initArguments" : chaincode_init_arguments,
    #           "created" : chaincode_created})


    chConfig["contracts"] = [{"id" : chaincodeName,
                "contractID" : chaincodeName,
               "install": {
                    "version" : '"v{}"'.format(chaincodeversion),
                     "language" : chaincode_lang,
                    "path" : chaincode_path,},
               'instantiate': {'initFunction': chaincode_init_function},
               "initArguments" : chaincode_init_arguments,
               "created" : chaincode_created,
               "endorsementPolicy" : {'policy': {'{}-of'.format(len(orgNames)): [{'signed-by': x} for x in range(len(orgNames))]},
               "identities" : contractsIdentitiesList(orgNames)}}]

    #caliperNetwork
    # can use here endorsersList
    #{'policy': {'{}}-of'.format(len(orgNames)): [{'signed-by': x} for x in range(len(orgNames))]}}
                                                
    #chConfig["contracts"]["endorsementPolicy"] = {'policy': {'{}-of'.format(len(orgNames)): [{'signed-by': x} for x in range(len(orgNames))]}}
    #chConfig["contracts"]["identities"] = contractsIdentitiesList(orgNames)

    return chConfig


def genCaliperOrganizations(domainName, orgsCount, orderersCount, peerCounts, orgNames):
    config = []
    for org in orgNames:
        orgConfig = {}
        orgConfig["mspid"] = "{}MSP".format(org)
        orgConfig["identities"] = {'certificates': [{'admin': True, 'clientPrivateKey': {'path': 'secret/{}/tls/admin.pem'.format(org)}, 'clientSignedCert': {'path': 'secret/{}/tls/admin.cert'.format(org)}, 'name': '"admin"'}]}
        orgConfig["connectionProfile"] = {'path': '"./{}ConnectionProfile.yaml"'.format(org), 'discover': "True"}

        config.append(orgConfig)
    return config


def getCaliperNetworkConfig(domainName, orgsCount, orderersCount, chaincodeName, peerCounts, chaincodeversion, chaincode_lang, chaincode_init_function, chaincode_path, orgNames, chaincode_init_arguments, chaincode_created, endorsersList):



    caliperConfig = {}
    caliperConfig["channels"] = [genCaliperChannels(domainName, orgsCount, orderersCount, chaincodeName, peerCounts, chaincodeversion, chaincode_lang, chaincode_init_function, chaincode_path, endorsersList, chaincode_init_arguments, chaincode_created)]

    #streamChannel = yaml.dump(caliperConfig, default_flow_style = None, sort_keys=False)
    #fHandle.write(stream.replace("'", ""))
    #fHandle.write(streamChannel)

    caliperConfig["info"] = {"Version" : "2.2.0",
                             "Size" : "3 Orgs",
                             "Orderer" : "Kafka",
                             "Distribution" : "Single Host",
                             "StateDB" : "CouchDB"}

    caliperConfig["caliper"] = {'sutOptions': {'mutualTls': False}, 'blockchain': 'fabric', 'fabric': {'gateway': {'usegateway': True, 'discovery': True}}}
    caliperConfig["version"] ='"2.0.0"'
    caliperConfig["name"] = "Fabric"


    caliperConfig["organizations"] = genCaliperOrganizations(domainName, orgsCount, orderersCount, peerCounts, endorsersList)

    fHandle = open("caliperNetworkConfig.yaml", "w")
    stream = yaml.dump(caliperConfig, default_flow_style = False, sort_keys=False)
    fHandle.write(stream.replace("'", ""))
    #fHandle.write(stream)
    fHandle.close()

'''
    

    caliperConfig["peers"] = genCaliperPeers(domainName, orgsCount, orderersCount, peerCounts)


    caliperConfig["orderers"] = genCaliperOrderers(domainName, orgsCount, orderersCount)





    caliperConfig["clients"] = genCaliperClients(domainName, orgsCount, orderersCount, peerCounts)



def peersListInsideOrganizations(org, peerCounts):

    config = []

    #for peerPerOrg in range(sum(peerCounts[0:org]), sum(peerCounts[0:org])+peerCounts[org]):
    for peerPerOrg in range(peerCounts[org]):
        #print peerCounts, org,  peerPerOrg, sum(peerCounts[0:org]), sum(peerCounts[0:org])+peerCounts[org]
        config.append("fabric-peer{}org{}".format(peerPerOrg+1,org+1))

    return config


def genCaliperOrderers(domainName, orgsCount, orderersCount):
    ordererConfig = {}
    for ordcounter in range(orderersCount):
        
        ordererConfig["ord{}-hlf-ord".format(ordcounter+1)] = {"url" : "grpc://fabric-ord{}:7050".format(ordcounter+1),
                                                               "grpcOptions" : {"ssl-target-name-override" : "fabric-ord{}".format(ordcounter+1)}}

    return ordererConfig

def genCaliperPeers(domainName, orgsCount, orderersCount, peerCounts):
    clConfig = {}
    for org in range(orgsCount):
        #for peerPerOrg in range(sum(peerCounts[0:org]), sum(peerCounts[0:org])+peerCounts[org]):
        for peerPerOrg in range(peerCounts[org]):

            clConfig["fabric-peer{}org{}".format(peerPerOrg+1,org+1)] = {"url" : "grpc://fabric-peer{}org{}:7051".format(peerPerOrg+1, org+1), "grpcOptions" : {"ssl-target-name-override" : "fabric-peer{}org{}".format(peerPerOrg+1,org+1),
"grpc.keepalive_time_ms" : "600000"}}

        #print peerCounts, org,  peerPerOrg, sum(peerCounts[0:org]), sum(peerCounts[0:org])+peerCounts[org]
    return clConfig
'''




"""
def gendependencies(orgsCount, orderersCount, peerCounts):
    config = []
    for ordcounter in range(orderersCount):
        ordererConfig = {}
        ordererConfig["name"] = "hlf-ord"
        ordererConfig["version"] = "1.3.0"
        ordererConfig["repository"] = "https://kubernetes-charts.storage.googleapis.com/"
        ordererConfig["alias"] = "ord{}".format(ordcounter+1)
        config.append(ordererConfig)


    for org in range(orgsCount):
        for peerNum in range(peerCounts[org]):
            orgConfig = {}
            orgConfig["name"] = "hlf-peer"
            orgConfig["version"] = "1.3.0"
            orgConfig["repository"] = "https://kubernetes-charts.storage.googleapis.com/"
            orgConfig["alias"] = "peer{}org{}".format(peerNum+1,org+1)
            config.append(orgConfig)


    for cdb in range(orgsCount):
        for peerNum in range(peerCounts[cdb]):
            #print(peerCounts[cdb])
            cdbConfig = {}
            cdbConfig["name"] = "hlf-couchdb"
            cdbConfig["version"] = "1.0.7"
            cdbConfig["repository"] = "https://kubernetes-charts.storage.googleapis.com/"
            cdbConfig["alias"] = "cdb-peer{}org{}".format(peerNum+1,cdb+1)
            config.append(cdbConfig)


    kafkaConfig = {}
    kafkaConfig["name"] = "kafka"
    kafkaConfig["version"] = "0.20.8"
    kafkaConfig["repository"] = "https://kubernetes-charts-incubator.storage.googleapis.com/"
    kafkaConfig["alias"] = "kafka-hlf"
    config.append(kafkaConfig)

    return config
"""


"""
def genFabricrequirements(orgsCount, orderersCount, peerCounts):
    reqConfig = {}
    reqConfig["dependencies"] = gendependencies(orgsCount, orderersCount, peerCounts)

    fHandle = open("requirements.yaml", "w")

    stream = yaml.dump(reqConfig, default_flow_style = False)
    stream2 = stream.replace("  ", "    ")
    fHandle.write(stream2.replace("- ", "  - "))


    #fHandle.write(yaml.dump(reqConfig, default_flow_style=False))
    fHandle.close()
"""

"""
def genFabricValues(orgsCount, orderersCount, peerCounts):
    config = {}
    for cdb in range(orgsCount):
        for peerNum in range(peerCounts[cdb]):
        #config = {}
            config["cdb-peer{}org{}".format(peerNum+1,cdb+1)] = {
                "fullnameOverride" : "cdb-peer{}org{}-hlf-couchdb".format(peerNum+1,cdb+1),
                "image" : {"tag" : "0.4.10"},
                "persistence" : {"size" : "1Gi",
                                 "storageClass" : "local-path"},
            }

        #config.append(cdbConfig)


    for org in range(orgsCount):# must add another for for peerCount[org+1]
        for peerNum in range(peerCounts[org]):
        #orgConfig = {}
            config["peer{}org{}".format(peerNum+1,org+1)] = {
            "image" : {"tag" : "1.4.6"},
            "persistence" : {"accessMode" : "ReadWriteOnce",
                             "size" : "1Gi",
                             "storageClass" : "local-path"},

            "peer" : {"databaseType" : "CouchDB",
                      "couchdbInstance" : "cdb-peer{}org{}".format(peerNum+1,org+1),
                      "mspID" : "Org{}MSP".format(org+1)},

            "secrets" : {"channels" : '[hlf--channel]',
                         "adminCert" : "hlf--peer{}-admincert".format(org+1),
                         "adminKey" : "hlf--peer{}-adminkey".format(org+1),
                         "peer" : {
                              "cert" : "hlf--peer{}--org{}-idcert".format(peerNum+1, org+1),#### here ### modify to be peer{peerCount[org]+1}{org+1}
                              "key" : "hlf--peer{}--org{}-idkey".format(peerNum+1, org+1),### here ###
                              "caCert" : "hlf--peer{}-cacert".format(org+1)}},

            "nodeSelector" : {
                      "peer" : "peer{}org{}".format(peerNum+1,org+1)}

        }

        #config.append(orgConfig)




    for ordcounter in range(orderersCount):
        #ordererConfig = {}
        config["ord{}".format(ordcounter+1)] = {
            "image" : {"tag" : "1.4.6"},
            "persistence" : {"enabled" : "true",
                             "accessMode" : "ReadWriteOnce",
                             "size" : "1Gi",
                             "storageClass" : "local-path"},

            "ord" : {"type" : "kafka",
                     "mspID" : "Orderer{}MSP".format(ordcounter+1)},

            "secrets" : {"ord" : {
                              "cert" : "hlf--ord{}-idcert".format(ordcounter+1),
                              "key" : "hlf--ord{}-idkey".format(ordcounter+1),
                              "caCert" : "hlf--ord{}-cacert".format(ordcounter+1)},
                         "genesis" : "hlf--genesis",
                         "adminCert" : "hlf--ord{}-admincert".format(ordcounter+1)}
        }


        #config.append(ordererConfig)

    #kafkaConfig = {}
    config["kafka-hlf"] = {
            "persistence" : {"enabled" : "true",
                             "accessMode" : "ReadWriteOnce",
                             "size" : "1Gi",
                             "storageClass" : "local-path"}}

    #config.append(kafkaConfig)



    #imageConfig = {}
    config["image"] = {
            "repository" : "nginx",
            "tag" : "stable",
            "pullPolicy" : "IfNotPresent"}

    #config.append(imageConfig)

    config["persistence"] = {
            "enabled" : "true",
            "annotations" : "{}",
            "storageClass" : "local-path",
            "accessMode" : "ReadWriteOnce",
            "size" : "1Gi"}

    #serviceAccountConfig = {}
    config["serviceAccount"] = {
            "create" : "true",
            "name" : '""'}

    #config.append(serviceAccountConfig)


    #servicConfig = {}
    config["service"] = {
            "type" : "ClusterIP",
            "port" : "80"}

    #config.append(servicConfig)


    #ingressConfig = {}
    config["ingress"] = {
            "enabled" : "false",
            "annotations" : "{}",
            "hosts" : {"host" : "chart-example.local" , "paths" : "[]"},
            "tls" : "[]"}

    #config.append(ingressConfig)




    config["replicaCount"] = "1"
 
    config["imagePullSecrets"] = "[]"

    config["nameOverride"] = '""'
    config["fullnameOverride"] = '""'
    config["resources"] = '{}'
    config["nodeSelector"] = '{}'
    config["tolerations"] = '[]'
    config["affinity"] = '{}'

    fHandle = open("values.yaml", "w")


    stream = yaml.dump(config, default_flow_style = False)
    #stream2 = stream.replace("- ", "")
    #stream3 = stream2.replace("    ", "  ")
    fHandle.write(stream.replace("'", ""))


    #fHandle.write(yaml.dump(config, default_flow_style=False))
    fHandle.close()


    """






'''
def genCaliperOrderers(domainName, orgsCount, orderersCount):
    ordererConfig = {}
    for ordcounter in range(orderersCount):
        
        ordererConfig["ord{}-hlf-ord".format(ordcounter+1)] = {"url" : "grpc://fabric-ord{}:7050".format(ordcounter+1),
                                                               "grpcOptions" : {"ssl-target-name-override" : "fabric-ord{}".format(ordcounter+1)}}

    return ordererConfig

def genCaliperPeers(domainName, orgsCount, orderersCount, peerCounts):
    clConfig = {}
    for org in range(orgsCount):
        #for peerPerOrg in range(sum(peerCounts[0:org]), sum(peerCounts[0:org])+peerCounts[org]):
        for peerPerOrg in range(peerCounts[org]):

            clConfig["fabric-peer{}org{}".format(peerPerOrg+1,org+1)] = {"url" : "grpc://fabric-peer{}org{}:7051".format(peerPerOrg+1, org+1), "grpcOptions" : {"ssl-target-name-override" : "fabric-peer{}org{}".format(peerPerOrg+1,org+1),
"grpc.keepalive_time_ms" : "600000"}}

        #print peerCounts, org,  peerPerOrg, sum(peerCounts[0:org]), sum(peerCounts[0:org])+peerCounts[org]
    return clConfig
''' 



def getCaliperConnectionProfile(domainName, orgsCount, orderersCount, chaincodeName, peerCounts, chaincodeversion, chaincode_lang, chaincode_init_function, chaincode_path, org, ordererOrg, ordererIndexCounter):

    caliperConnectionProfile = {}
    #connectionProfile = {'organizations': {'carrier-net{var OrgName}': {'peers': ['peer0.carrier-net'], 'orderers': ['orderer1.supplychain-net'], 'mspid': 'carrierMSP', 'certificateAuthorities': ['ca.carrier-net']}}, 'peers': {'peer0.carrier-net': {'url': 'grpcs://peer0.carrier-net:7051', 'tlsCACerts': {'path': 'secret/msp/tlscacerts/tlsca.pem'}}}, 'orderers': {'orderer1.supplychain-net': {'url': 'grpcs://orderer1.supplychain-net:7050', 'tlsCACerts': {'path': 'secret/msp/tlscacerts/orderer-tlsca.pem'}}}, 'name': 'test-network-carrier-net', 'certificateAuthorities': {'ca.carrier-net': {'url': 'https://ca.carrier-net:7054', 'httpOptions': {'verify': False}, 'tlsCACerts': {'path': 'secret/msp/tlscacerts/tlsca.pem'}, 'caName': 'ca.carrier-net'}}, 'channels': {'allchannel': {'peers': {'peer0.carrier-net': {'endorsingPeer': True, 'chaincodeQuery': True, 'eventSource': True, 'discover': True, 'ledgerQuery': True}}, 'orderers': ['orderer1.supplychain-net']}}, 'client': {'organization': 'carrier-net', 'connection': {'timeout': {'peer': {'endorser': '300', 'eventHub': '300', 'eventReg': '300'}, 'orderer': '300'}}}, 'version': '1.0.0'}
    caliperConnectionProfile["channels"] = {'allchannel': {'peers': {'peer0.{}-net'.format(org): {'endorsingPeer': True, 'chaincodeQuery': True, 'eventSource': True, 'discover': True, 'ledgerQuery': True}}, 'orderers': ['orderer{}.{}-net'.format(ordererIndexCounter, ordererOrg)]}}
    caliperConnectionProfile["name"] = "test-network-carrier-net"
    caliperConnectionProfile["version"] = "1.0.0"
    caliperConnectionProfile["client"] = {'organization': '{}-net'.format(org), 'connection': {'timeout': {'peer': {'endorser': '"300"', 'eventHub': '"300"', 'eventReg': '"300"'}, 'orderer': '"300"'}}}

    caliperConnectionProfile["peers"] = {'peer0.{}-net'.format(org): {'url': 'grpcs://peer0.{}-net:7051'.format(org), 'tlsCACerts': {'path': 'secret/{}/msp/tlscacerts/tlsca.pem'.format(org)}}}

    ###########" get the ordere org of this ogr here for fabric config"
    caliperConnectionProfile["orderers"] = {'orderer{}.{}-net'.format(ordererIndexCounter, ordererOrg): {'url': 'grpcs://orderer{}.{}-net:7050'.format(ordererIndexCounter,ordererOrg), 'tlsCACerts': {'path': 'secret/{}/msp/tlscacerts/orderer-tlsca.pem'.format(org)}}}

    caliperConnectionProfile["organizations"] = {'{}-net'.format(org): {'peers': ['peer0.{}-net'.format(org)], 'orderers': ['orderer{}.{}-net'.format(ordererIndexCounter, ordererOrg)], 'mspid': '{}MSP'.format(org), 'certificateAuthorities': ['ca.{}-net'.format(org)]}}
    
    caliperConnectionProfile["certificateAuthorities"] = {'ca.{}-net'.format(org): {'url': 'https://ca.{}-net:7054'.format(org), 'httpOptions': {'verify': False}, 'tlsCACerts': {'path': 'secret/{}/msp/tlscacerts/tlsca.pem'.format(org)}, 'caName': 'ca.{}-net'.format(org)}}


    #caliperConfig["clients"] = genCaliperClients(domainName, orgsCount, orderersCount, peerCounts)



    fHandle = open("{}ConnectionProfile.yaml".format(org), "w")
    stream = yaml.dump(caliperConnectionProfile, default_flow_style = False, sort_keys=False)
    fHandle.write(stream.replace("'", ""))
    fHandle.close()






def genBAFChannels(domainName, orgsCount, orderersCount, peerCounts, orgNames):
    config = []
    for org in orgNames:
        orgConfig = {}
        orgConfig["mspid"] = "{}MSP".format(org)
        orgConfig["identities"] = {'certificates': [{'admin': True, 'clientPrivateKey': {'path': "secret/{}/tls/admin.pem".format(org)}, 'clientSignedCert': {'path': "secret/{}/tls/admin.cert".format(org)}, 'name': 'admin'}]}
        orgConfig["connectionProfile"] = {'path': './{}ConnectionProfile.yaml'.format(org), 'discover': "True"}

        config.append(orgConfig)
    return config







def genBAForganizationsPeers(org, peerCounts, orgNames, pathToBAF, chaincodeversion, chaincodeName, BAFgitusername, BAFgit_url, BAFgitpassword, BAFgitbranch, BAFChaincodePath):
    config = []
    for peerNb in range(peerCounts[org]):
        
        peerConfig={'name': 'peer{}'.format(peerNb), 'certificate': '{}/build/{}/ca.crt'.format(pathToBAF, orgNames[org]),
            'peerAddress': 'peer{}.{}-net:7051'.format(peerNb, orgNames[org]), 'grpc': {'port': 7051}, 'restserver': {'targetPort': 20001, 'port': 20001},
            'couchdb': {'port': 5984}, 'gossippeeraddress': 'peer{}.{}-net:7051'.format((peerNb+1) % peerCounts[org], orgNames[org]), 'chaincode': {'version': '"{}"'.format(chaincodeversion),
            'name': '"{}"'.format(chaincodeName), 'repository': {'username': '"{}"'.format(BAFgitusername), 'url': '"{}"'.format(BAFgit_url),
            'password': '"{}"'.format(BAFgitpassword), 'branch': '{}'.format(BAFgitbranch), 'path': '"{}"'.format(BAFChaincodePath)},
            'endorsements': '""', 'maindirectory': '"."', 'arguments': '\\"init\\",\\"\\"'}, 'peer': '', 'type': 'anchor',
            'events': {'port': 7053}, 'expressapi': {'targetPort': 3000, 'port': 3000}, 'cli': 'disabled'}
        config.append(peerConfig)
    return config




    

def genBAFOrganizations(domainName, orgsCount, orderersCount, peerCounts, orgNames, BAFgit_protocol, BAFgit_url,
BAFgitbranch, BAFgitrelease_dir, BAFgitchart_source, BAFgit_repo, BAFgitusername, BAFgitpassword,
BAFgitemail, BAFgitprivate_key, BAFk8sContext, BAFk8sConfig_file, VAULT_ADDR, VAULT_TOKEN, endorsersList, ordererOwnershipList,
pathToBAF, chaincodeversion, chaincodeName, BAFChaincodePath, cloud_provider):

    config = []
    for org in range(len(orgNames)):
        if orgNames[org] in endorsersList:
            orgConfig = {'ca_data': {'url': 'ca.{}-net:7054'.format(orgNames[org]), 'certificate': 'file/server.crt'},
            'org_status': 'new', 'external_url_suffix': 'develop.local.com', 'organization': '',

            'services': {'ca': {'grpc': {'port': 7054}, 'type': 'ca', 'name': 'ca',
            'subject': '/C=CH/ST=Zurich/L=Zurich/O={}/CN=ca.{}-net'.format(orgNames[org], orgNames[org])},
            
            'peers': genBAForganizationsPeers(org, peerCounts, orgNames, pathToBAF, chaincodeversion, chaincodeName, BAFgitusername, BAFgit_url, BAFgitpassword, BAFgitbranch, BAFChaincodePath)},
            
            'k8s': {'region': '"cluster_region"', 'config_file': '"{}"'.format(BAFk8sConfig_file), 'context': '"{}"'.format(BAFk8sContext)},
            
            'cli': 'enabled',

            'gitops': {'username': '"{}"'.format(BAFgitusername),
            'private_key': '"{}/build/gitops"'.format(pathToBAF),
            'git_protocol': '"{}"'.format(BAFgit_protocol), 'chart_source': '"{}"'.format(BAFgitchart_source),
            'git_url': '"{}://{}"'.format(BAFgit_protocol, BAFgit_url),
            'branch': '"{}"'.format(BAFgitbranch), 'release_dir': '"{}"'.format(BAFgitrelease_dir),
            'password': '"{}"'.format(BAFgitpassword), 'email': '"{}"'.format(BAFgitemail), 'git_repo': '"{}"'.format(BAFgit_repo)},

            'cloud_provider': '{}'.format(cloud_provider), 'name': orgNames[org], 'country': 'CH', 'state': 'Zurich',
            'location': 'Zurich', 'vault': {'url': '"{}"'.format(VAULT_ADDR), 'root_token': '"{}"'.format(VAULT_TOKEN),
            'secret_path': '"secret"'},

            'subject': 'O={},OU={},L=47.38/8.54/Zurich,C=CH'.format(orgNames[org], orgNames[org]), 'type': 'peer'}
            
        else:
            orgConfig = {'ca_data': {'url': 'ca.{}-net:7054'.format(orgNames[org]), 'certificate': 'file/server.crt'},
            'org_status': 'new', 'external_url_suffix': 'develop.local.com', 'organization': '',

            'services': {'consensus': {'grpc': {'port': 9092}, 'type': 'broker', 'name': 'kafka', 'replicas': 3},
            'ca': {'grpc': {'port': 7054}, 'type': 'ca', 'name': 'ca',
            'subject': '"/C=GB/ST=London/L=London/O=Orderer/CN=ca.{}-net"'.format(orgNames[org], orgNames[org])},


            'orderers': [{'consensus': 'kafka', 'grpc': {'port': 7050}, 'orderer': '', 'type': 'orderer',
            'name': 'orderer{}'.format(x)} for x in range(peerCounts[orgNames.index(ordererOwnershipList[org])])]},
            
            'k8s': {'region': '"cluster_region"', 'config_file': '"{}"'.format(BAFk8sConfig_file), 'context': '"{}"'.format(BAFk8sContext)},
            'cli': 'disabled',
            
            'gitops': {'username': '"{}"'.format(BAFgitusername),
            'private_key': '"{}/build/gitops"'.format(pathToBAF),
            'git_protocol': '"{}"'.format(BAFgit_protocol), 'chart_source': '"{}"'.format(BAFgitchart_source),
            'git_url': '"{}://{}"'.format(BAFgit_protocol, BAFgit_url),
            'branch': '"{}"'.format(BAFgitbranch), 'release_dir': '"{}"'.format(BAFgitrelease_dir),
            'password': '"{}"'.format(BAFgitpassword), 'email': '"{}"'.format(BAFgitemail), 'git_repo': '"{}"'.format(BAFgit_repo)},

            'cloud_provider': '{}'.format(cloud_provider), 'name': orgNames[org], 'country': 'CH', 'state': 'Zurich',
            'location': 'Zurich', 'vault': {'url': '"{}"'.format(VAULT_ADDR), 'root_token': '"{}"'.format(VAULT_TOKEN),
            'secret_path': '"secret"'},
            
            'subject': '"O=Orderer,L=51.50/-0.13/London,C=GB"', 'type': 'orderer'}
            


        #orgConfig = {'cloud_provider': 'minikube', 'name': org, 'country': 'GB', 'state': 'London', 'location': 'London', 'subject': 'O=Carrier,OU=Carrier,L=51.50/-0.13/London,C=GB', 'type': 'peer', 'version': '2.2.0'}
        #orgConfig["organization"] = None
        #orgConfig["services"] = {'ca': {'grpc': {'port': 7054}, 'type': 'ca', 'name': 'ca', 'subject': '/C=GB/ST=London/L=London/O=Carrier/CN=ca.carrier-net'}}
        # TAKE peer count per org into cconsideration here ******* and in caliper config ****************
        #orgConfig["peers"] = None
        #orgConfig["vault"] = {'url': vaultUrl, 'root_token': vaultRootToken, 'secret_path': 'secret'}

        config.append(orgConfig)
    return config


def genBAFOrderers(ordererOwnershipList, pathToBAF, peerCounts, orgNames):
    config = []
    used = []
    for org in range(len(ordererOwnershipList)):
        if ordererOwnershipList[org] not in used:
            for orderer in range(peerCounts[orgNames.index(ordererOwnershipList[org])]):
                ordererConfig={'org_name': ordererOwnershipList[org], 'orderer': '', 'name': 'orderer{}'.format(orderer),
                    'certificate': '{}/build/{}/orderer{}.crt'.format(pathToBAF, ordererOwnershipList[org], orderer), 'type': 'orderer',
                    'uri': 'orderer{}.{}-net:7050'.format(orderer, ordererOwnershipList[org])}
                config.append(ordererConfig)
                used.append(ordererOwnershipList[org])
    return config

def TOUSEEEEchannelParticipantsList(orgNames):
    mspConfig=''
    for x in range(len(orgNames)):
        if x !=0:
            mspConfig = mspConfig + ', "{}MSP"'.format(orgNames[x])
        else:
            mspConfig = mspConfig + '"{}MSP"'.format(orgNames[x])

        #mspConfig.append('{}MSP'.format(x))
    return '[{}]'.format(mspConfig)










def channelParticipantsList(orgNames, ordererOwnershipList, peerCounts, endorsersList):
    config = []
    ordererIndexCounter = 0
    for org in range(len(orgNames)):
        if orgNames[org] in endorsersList:
            a = [1,2,3]
            orgConfig = {'ordererAddress': 'orderer{}.{}-net:7050'.format(ordererIndexCounter, ordererOwnershipList[org]),
            'peers': [{'peer': '', 'gossipAddress': 'peer{}.{}-net:7051'.format((x+1) % peerCounts[org], orgNames[org]),
            'name': 'peer{}'.format(x), 'peerAddress': 'peer{}.{}-net:7051'.format(x , orgNames[org])} for x in range(peerCounts[org])],
            'name': orgNames[org], 'organization': '', 'org_status': 'new'}
            #for x in range(peerCounts[org]):
            #    print("********************* HERE ************ peer{}.{}-net:7051.format(x+1 % peerCounts[org]", org, x, x+1, peerCounts[org], (x+1) % peerCounts[org])
            if org == 0:
                orgConfig["type"] = "creator"
            else:
                orgConfig["type"] = "joiner"
            config.append(orgConfig)
            ordererIndexCounter += 1
    return config





def getBAFnetwork(domainName, orgsCount, orderersCount, chaincodeName, peerCounts, chaincodeversion, chaincode_lang, chaincode_init_function, chaincode_path, orgNames, BAFgit_protocol, BAFgit_url, BAFgitbranch, BAFgitrelease_dir, BAFgitchart_source, BAFgit_repo, BAFgitusername, BAFgitpassword, BAFgitemail, BAFgitprivate_key, BAFk8sContext, BAFk8sConfig_file, vaultUrl, vaultRootToken, endorsersList, ordererOwnershipList, pathToBAF, BAFChaincodePath, cloud_provider):


    bafNetwork = {'network': {'channels': [{'channel_name': 'AllChannel', 'orderer': {'name': 'supplychain'},
    'participants': channelParticipantsList(orgNames, ordererOwnershipList, peerCounts, endorsersList),

    'endorsers': {'corepeerAddress': ['peer0.{}-net:7051'.format(x) for x in endorsersList],
    'name': endorsersList}, 
    
    'consortium': 'SupplyChainConsortium', 'channel': '',
    'genesis': {'name': 'OrdererGenesis'}}], 
    
    
    
    'organizations': genBAFOrganizations(domainName, orgsCount, orderersCount, peerCounts, orgNames, BAFgit_protocol, BAFgit_url,
    BAFgitbranch, BAFgitrelease_dir, BAFgitchart_source, BAFgit_repo, BAFgitusername, BAFgitpassword,
    BAFgitemail, BAFgitprivate_key, BAFk8sContext, BAFk8sConfig_file, vaultUrl, vaultRootToken, endorsersList, ordererOwnershipList, pathToBAF, chaincodeversion, chaincodeName, BAFChaincodePath, cloud_provider),




    'version': '2.2.0', 'orderers': genBAFOrderers(ordererOwnershipList, pathToBAF, peerCounts, orgNames), 'env': {'retry_count': 50, 'type': '"local"', 'proxy': 'none',
    'ambassadorPorts': '15010,15020', 'external_dns': 'disabled'}, 'docker': {'url': '"index.docker.io/hyperledgerlabs"',
    'username': '"docker_username"', 'password': '"docker_password"'}, 'type': 'fabric'}}


















    #bafNetwork = {}
    
    #bafNetwork["network"] = {'version': '2.2.0' ,'type': 'fabric'}
    #bafNetwork["network"]["env"] ={'env': {'retry_count': 50, 'type': 'local', 'proxy': 'none', 'ambassadorPorts': '15010,15020', 'external_dns': 'disabled'}, 'docker': {'url': 'index.docker.io/hyperledgerlabs', 'username': 'docker_username', 'password': 'docker_password'}} 
    ##bafNetwork["network"]["type"] = "fabric"
    ##bafNetwork["network"]["version"] = {"2.2.0"}

    #bafNetwork["network"]["channels"] = genBAFChannels(domainName, orgsCount, orderersCount, peerCounts, orgNames)


    #bafNetwork["network"]["orderers"] = genBAFOrderers(domainName, orgsCount, orderersCount)



    #bafNetwork["network"]["organizations"] = [genBAFOrganizations(domainName, orgsCount, orderersCount, peerCounts, orgNames, BAFgit_protocol, BAFgit_url, BAFgitbranch, BAFgitrelease_dir, BAFgitchart_source, BAFgit_repo, BAFgitusername, BAFgitpassword, BAFgitemail, BAFgitprivate_key, BAFk8sContext, BAFk8sConfig_file, vaultUrl, vaultRootToken)]


    ##{'network': {'channels': [{'channel_name': 'AllChannel', 'orderer': {'name': 'supplychain'}, 'participants': [{'ordererAddress': 'orderer1.supplychain-net:7050', 'peers': [{'peer': None, 'gossipAddress': 'peer0.carrier-net:7051', 'name': 'peer0', 'peerAddress': 'peer0.carrier-net:7051'}], 'name': 'carrier', 'organization': None, 'type': 'creator', 'org_status': 'new'}, {'ordererAddress': 'orderer1.supplychain-net:7050', 'peers': [{'peer': None, 'gossipAddress': 'peer0.manufacturer-net:7051', 'name': 'peer0', 'peerAddress': 'peer0.manufacturer-net:7051'}], 'name': 'manufacturer', 'organization': None, 'type': 'joiner', 'org_status': 'new'}], 'endorsers': {'corepeerAddress': ['peer0.carrier-net:7051', 'peer0.manufacturer-net:7051'], 'name': ['carrier', 'manufacturer']}, 'consortium': 'SupplyChainConsortium', 'channel': None, 'genesis': {'name': 'OrdererGenesis'}}], 'organizations': [{'ca_data': {'url': 'ca.supplychain-net:7054', 'certificate': 'file/server.crt'}, 'org_status': 'new', 'external_url_suffix': 'develop.local.com', 'organization': None, 'services': {'consensus': {'grpc': {'port': 9092}, 'type': 'broker', 'name': 'kafka', 'replicas': 3}, 'ca': {'grpc': {'port': 7054}, 'type': 'ca', 'name': 'ca', 'subject': '/C=GB/ST=London/L=London/O=Orderer/CN=ca.supplychain-net'}, 'orderers': [{'consensus': 'kafka', 'grpc': {'port': 7050}, 'orderer': None, 'type': 'orderer', 'name': 'orderer1'}]}, 'k8s': {'region': 'cluster_region', 'config_file': '~/.kube/config', 'context': 'local'}, 'cli': 'disabled', 'gitops': {'username': 'ayhamkassab', 'private_key': '/root/hyperledger-labs/myFork/blockchain-automation-framework/build/gitops', 'git_protocol': 'https', 'chart_source': 'platforms/hyperledger-fabric/charts', 'git_url': 'https://github.com/ayhamkassab/blockchain-automation-framework.git', 'branch': 'master', 'release_dir': 'platforms/hyperledger-fabric/releases/dev', 'password': '**Ak47**', 'email': 'github_email', 'git_repo': 'github.com/ayhamkassab/blockchain-automation-framework.git'}, 'cloud_provider': 'minikube', 'name': 'supplychain', 'country': 'UK', 'state': 'London', 'location': 'London', 'vault': {'url': 'http://130.104.229.21:30000', 'root_token': 's.32JqFMwzbrqbeFl8bKI9R2QC', 'secret_path': 'secret'}, 'subject': 'O=Orderer,L=51.50/-0.13/London,C=GB', 'type': 'orderer'}, {'ca_data': {'url': 'ca.manufacturer-net:7054', 'certificate': 'file/server.crt'}, 'org_status': 'new', 'external_url_suffix': 'develop.local.com', 'organization': None, 'services': {'ca': {'grpc': {'port': 7054}, 'type': 'ca', 'name': 'ca', 'subject': '/C=CH/ST=Zurich/L=Zurich/O=Manufacturer/CN=ca.manufacturer-net'}, 'peers': [{'name': 'peer0', 'certificate': '/root/hyperledger-labs/myFork/blockchain-automation-framework/build/manufacturer/ca.crt', 'peerAddress': 'peer0.manufacturer-net:7051', 'grpc': {'port': 7051}, 'restserver': {'targetPort': 20001, 'port': 20001}, 'couchdb': {'port': 5984}, 'gossippeeraddress': 'peer0.manufacturer-net:7051', 'chaincode': {'version': '1', 'name': 'simpleauction', 'repository': {'username': 'ayhamkassab', 'url': 'github.com/ayhamkassab/blockchain-automation-framework.git', 'password': '**Ak47**', 'branch': 'master', 'path': 'examples/supplychain-app/fabric/chaincode_rest_server/chaincode/'}, 'endorsements': '', 'maindirectory': '.', 'arguments': '\\"init\\",\\"\\"'}, 'peer': None, 'type': 'anchor', 'events': {'port': 7053}, 'expressapi': {'targetPort': 3000, 'port': 3000}, 'cli': 'disabled'}]}, 'k8s': {'region': 'cluster_region', 'config_file': '~/.kube/config', 'context': 'local'}, 'cli': 'enabled', 'gitops': {'username': 'ayhamkassab', 'private_key': '/root/hyperledger-labs/myFork/blockchain-automation-framework/build/gitops', 'git_protocol': 'https', 'chart_source': 'platforms/hyperledger-fabric/charts', 'git_url': 'https://github.com/ayhamkassab/blockchain-automation-framework.git', 'branch': 'master', 'release_dir': 'platforms/hyperledger-fabric/releases/dev', 'password': '**Ak47**', 'email': 'github_email', 'git_repo': 'github.com/ayhamkassab/blockchain-automation-framework.git'}, 'cloud_provider': 'minikube', 'name': 'manufacturer', 'country': 'CH', 'state': 'Zurich', 'location': 'Zurich', 'vault': {'url': 'http://130.104.229.21:30000', 'root_token': 's.32JqFMwzbrqbeFl8bKI9R2QC', 'secret_path': 'secret'}, 'subject': 'O=Manufacturer,OU=Manufacturer,L=47.38/8.54/Zurich,C=CH', 'type': 'peer'}, {'ca_data': {'url': 'ca.carrier-net:7054', 'certificate': 'file/server.crt'}, 'org_status': 'new', 'external_url_suffix': 'develop.local.com', 'organization': None, 'services': {'ca': {'grpc': {'port': 7054}, 'type': 'ca', 'name': 'ca', 'subject': '/C=GB/ST=London/L=London/O=Carrier/CN=ca.carrier-net'}, 'peers': [{'name': 'peer0', 'certificate': '/root/hyperledger-labs/myFork/blockchain-automation-framework/build/carrier/ca.crt', 'peerAddress': 'peer0.carrier-net:7051', 'grpc': {'port': 7051}, 'restserver': {'targetPort': 20001, 'port': 20001}, 'couchdb': {'port': 5984}, 'gossippeeraddress': 'peer0.carrier-net:7051', 'chaincode': {'version': '1', 'name': 'simpleauction', 'repository': {'username': 'ayhamkassab', 'url': 'github.com/ayhamkassab/blockchain-automation-framework.git', 'password': '**Ak47**', 'branch': 'master', 'path': 'examples/supplychain-app/fabric/chaincode_rest_server/chaincode/'}, 'endorsements': '', 'maindirectory': '.', 'arguments': '\\"init\\",\\"\\"'}, 'peer': None, 'type': 'anchor', 'events': {'port': 7053}, 'expressapi': {'targetPort': 3000, 'port': 3000}, 'cli': 'disabled'}]}, 'k8s': {'region': 'cluster_region', 'config_file': '~/.kube/config', 'context': 'local'}, 'cli': 'enabled', 'gitops': {'username': 'ayhamkassab', 'private_key': '/root/hyperledger-labs/myFork/blockchain-automation-framework/build/gitops', 'git_protocol': 'https', 'chart_source': 'platforms/hyperledger-fabric/charts', 'git_url': 'https://github.com/ayhamkassab/blockchain-automation-framework.git', 'branch': 'master', 'release_dir': 'platforms/hyperledger-fabric/releases/dev', 'password': '**Ak47**', 'email': 'github_email', 'git_repo': 'github.com/ayhamkassab/blockchain-automation-framework.git'}, 'cloud_provider': 'minikube', 'name': 'carrier', 'country': 'GB', 'state': 'London', 'location': 'London', 'vault': {'url': 'http://130.104.229.21:30000', 'root_token': 's.32JqFMwzbrqbeFl8bKI9R2QC', 'secret_path': 'secret'}, 'subject': 'O=Carrier,OU=Carrier,L=51.50/-0.13/London,C=GB', 'type': 'peer'}], 'version': '2.2.0', 'orderers': [{'org_name': 'supplychain', 'orderer': None, 'name': 'orderer1', 'certificate': '/root/hyperledger-labs/myFork/blockchain-automation-framework/build/orderer1.crt', 'type': 'orderer', 'uri': 'orderer1.supplychain-net:7050'}], 'env': {'retry_count': 50, 'type': 'local', 'proxy': 'none', 'ambassadorPorts': '15010,15020', 'external_dns': 'disabled'}, 'docker': {'url': 'index.docker.io/hyperledgerlabs', 'username': 'docker_username', 'password': 'docker_password'}, 'type': 'fabric'}}





    fHandle = open("bafNetwork.yaml", "w")
    stream = yaml.dump(bafNetwork, default_flow_style = False, sort_keys=False)
    fHandle.write(stream.replace("'", ""))
    fHandle.close()



def caliperValuesEndorsersList(orgNames):
    mspConfig=''
    for x in range(len(orgNames)):
        if x !=0:
            mspConfig = mspConfig + ' {}'.format(orgNames[x])
        else:
            mspConfig = mspConfig + '{}'.format(orgNames[x])

        #mspConfig.append('{}MSP'.format(x))
    return '"{}"'.format(mspConfig)


def getCaliperValues(replicaMasterCount, replicaWorkersCount, repository, vaultUrl, vaultRootToken, endorsersList):

    caliperValues = {'ingress': {'tls': [], 'enabled': False, 'hosts': [{'paths': [], 'host': 'chart-example.local'}],
    'annotations': {}}, 'replicaWorkersCount': replicaWorkersCount, 'image': {'pullPolicy': 'Always', 'tag': 'b2c2',
    'repository': repository}, 'args': {'second': 'manager', 'first': 'launch'},
    'replicaMasterCount': replicaMasterCount, 'VAULT_TOKEN': vaultRootToken, 'VAULT_ADDR': vaultUrl,
    'fullnameOverride': '', 'securityContext': {}, 'service': {'type': 'ClusterIP', 'port': 80},
    'serviceAccount': {'create': True, 'name': ''}, 'ENDORSING_ORG_NAMES': caliperValuesEndorsersList(endorsersList), 'podSecurityContext': {},
    'nodeSelector': {}, 'affinity': {}, 'nameOverride': '', 'tolerations': [], 'imagePullSecrets': [], 'resources': {}}



    fHandle = open("values.yaml", "w")
    stream = yaml.dump(caliperValues, default_flow_style = False, sort_keys=False)
    fHandle.write(stream.replace("'", ""))
    fHandle.close()






def generate():

    with open("fabricConfig.yaml", 'r') as stream:
        try:
            fabricConfig = yaml.safe_load(stream)

            endorsersBooleanList= []
            ordererOwnershipList= []
            ordererOwners= []
            peerCounts= []
            orgNames= []

            listOfOrgs = fabricConfig["organizations"]
            orgsCount= len(listOfOrgs)
            for org in range(orgsCount):
                ordererOwnershipList.append(listOfOrgs[org]["orderer"])
                orgNames.append(listOfOrgs[org]["name"])
                endorsersBooleanList.append(listOfOrgs[org]["endorser"])
                peerCounts.append(listOfOrgs[org]["numberOfPeers"])
                #if listOfOrgs[org]["endorser"]:
                #    peerCounts.append(listOfOrgs[org]["numberOfPeers"])
                #else:
                #    peerCounts.append(0)
            #orderersCount=(len([idx for idx in range(len(ordererOwnershipList)) if ordererOwnershipList[idx] == True]))
            orderersCount = len(np.unique(ordererOwnershipList))
            print(np.unique(ordererOwnershipList), orderersCount)
            domainName = fabricConfig["domain_name"]#"svc.cluster.local"
            endorsersList=([orgNames[idx] for idx in range(len(endorsersBooleanList)) if endorsersBooleanList[idx] == True])
            print(endorsersList, endorsersBooleanList, "peerCounts", peerCounts)

            BAFk8sContext = fabricConfig["BAFk8s"]["context"]
            BAFk8sConfig_file = fabricConfig["BAFk8s"]["config_file"]

            vaultUrl = fabricConfig["vault"]["url"]
            vaultRootToken = fabricConfig["vault"]["root_token"]
            pathToBAF = fabricConfig["pathToBAF"]
            cloud_provider = fabricConfig["cloud_provider"]
            BAFChaincodePath = fabricConfig["BAFChaincodePath"]
            

        except yaml.YAMLError as exc:
            print(exc)


    with open("gitops.yaml", 'r') as stream:
        try:
            gitopsConfig = yaml.safe_load(stream)

            BAFgit_protocol = gitopsConfig["BAFgitops"]["git_protocol"]
            BAFgit_url = gitopsConfig["BAFgitops"]["git_url"]
            BAFgitbranch = gitopsConfig["BAFgitops"]["branch"]
            BAFgitrelease_dir = gitopsConfig["BAFgitops"]["release_dir"]
            BAFgitchart_source = gitopsConfig["BAFgitops"]["chart_source"]
            BAFgit_repo = gitopsConfig["BAFgitops"]["git_repo"]
            BAFgitusername = gitopsConfig["BAFgitops"]["username"]
            BAFgitpassword = gitopsConfig["BAFgitops"]["password"]
            BAFgitemail = gitopsConfig["BAFgitops"]["email"]
            BAFgitprivate_key = gitopsConfig["BAFgitops"]["private_key"]
        except yaml.YAMLError as exc:
            print(exc)


    kafka=True


    #orgsCount = int(sys.argv[1])
    #orderersCount = int(sys.argv[2])
    zooKeepersCount = 3 if kafka else 0
    kafkasCount = 3 if kafka else 0
    #peerCounts =  list(sys.argv[3])#[2,1,3,1,1]21311
    #for x in range(len(peerCounts)):
    #    peerCounts[x]= int(peerCounts[x])


    with open("caliperConfig.yaml", 'r') as stream:
        try:
            caliperConfig = yaml.safe_load(stream)

            #chaincodeName = sys.argv[4]#simple# still not done in bench mark config yaml (so far only tps is modifyble there)
            chaincodeName = caliperConfig["chaincode_name"]
            chaincodeversion = caliperConfig["chaincode_version"]
            chaincode_lang = caliperConfig["chaincode_lang"]
            chaincode_init_function = caliperConfig["chaincode_init_function"]
            chaincode_path = caliperConfig["chaincode_path"]
            chaincode_init_arguments = caliperConfig["initArguments"]
            chaincode_created = caliperConfig["created"]
            replicaMasterCount = caliperConfig["replicaMasterCount"]
            replicaWorkersCount = caliperConfig["replicaWorkersCount"]
            caliperImageRepository = caliperConfig["caliperImageRepository"]
            
            #print("chaincode", chaincodeName)
            #print("chaincodeversion", chaincodeversion)
            #print("domain_name",domainName)
            #print("orgNames",orgNames)
            #print("orgsCount",orgsCount)
            #print("orderersCount",orderersCount)
            #print("peerCounts",peerCounts, type(peerCounts[0]))
        except yaml.YAMLError as exc:
            print(exc)


    #with open("microBenchMarkconfig.yaml", 'r') as stream:
    #    try:
    #        benchmarkConfig = yaml.safe_load(stream)
    #        print benchmarkConfig["test"]["rounds"][0]["txDuration"]
    #        print "****************************"
    #        print "****************************"
    #        print "****************************"
    #        benchmarkConfig["test"]["rounds"][0]["txDuration"] = caliperConfig["txDuration"]#now rewrite the file
    #        print benchmarkConfig["test"]["rounds"][0]["txDuration"]

    #        with open('microBenchMarkconfig.yaml', 'w') as outfile:
    #            yaml.dump(benchmarkConfig, outfile, default_flow_style=False)

    #    except yaml.YAMLError as exc:
    #        print(exc)

    #genFabricValues(orgsCount, orderersCount, peerCounts)

    #genFabricrequirements(orgsCount, orderersCount, peerCounts)

    getCaliperNetworkConfig(domainName, orgsCount, orderersCount, chaincodeName, peerCounts, chaincodeversion, chaincode_lang, chaincode_init_function, chaincode_path, orgNames, chaincode_init_arguments, chaincode_created, endorsersList)

    #for org in orgNames:

    ordererIndexCounter = 0
    for org in range(len(orgNames)):
        if orgNames[org] in endorsersList:
            getCaliperConnectionProfile(domainName, orgsCount, orderersCount, chaincodeName, peerCounts, chaincodeversion, chaincode_lang, chaincode_init_function, chaincode_path, orgNames[org], ordererOwnershipList[org], ordererIndexCounter)
            ordererIndexCounter += 1


    #for org in range(len(endorsersList)):
    #    getCaliperConnectionProfile(domainName, orgsCount, orderersCount, chaincodeName, peerCounts, chaincodeversion, chaincode_lang, chaincode_init_function, chaincode_path, endorsersList[org], ordererOwnershipList[org])

    getBAFnetwork(domainName, orgsCount, orderersCount, chaincodeName, peerCounts, chaincodeversion, chaincode_lang, chaincode_init_function, chaincode_path, orgNames, BAFgit_protocol, BAFgit_url, BAFgitbranch, BAFgitrelease_dir, BAFgitchart_source, BAFgit_repo, BAFgitusername, BAFgitpassword, BAFgitemail, BAFgitprivate_key, BAFk8sContext, BAFk8sConfig_file, vaultUrl, vaultRootToken, endorsersList, ordererOwnershipList, pathToBAF, BAFChaincodePath, cloud_provider)


    getCaliperValues(replicaMasterCount, replicaWorkersCount, caliperImageRepository, vaultUrl, vaultRootToken, endorsersList)
    #genNetwork(domainName, orgsCount, orderersCount, peerCounts)
    #genCrypto(domainName, orgsCount, orderersCount, peerCounts)

    #generateCerts()
    #replacePrivateKey(orgsCount, domainName)
    #generateChannelArtifacts(orgsCount)

    #generateHighThroughput(domainName, orgsCount, peerCounts)
    #generateDocker("hyperledger", "hyperledger-ov", domainName, orgsCount, orderersCount, peerCounts, zooKeepersCount, kafkasCount, "INFO")

def copytree(src, dst):
    if os.path.isdir(dst): shutil.rmtree(dst)
    shutil.copytree(src, dst)

def deploy():
    if os.path.isdir("/export"):
        copytree("./../high-throughput/scripts", "/export/scripts")
        copytree("./crypto-config", "/export/crypto-config")
        copytree("./channel-artifacts", "/export/channel-artifacts")
        # copytree("./../high-throughput/chaincode", "/export/chaincode")
        copytree("./../chaincode/thesis_chaincode", "/export/chaincode")

        subprocess.Popen(["chmod -R 777 /export"], shell=True).wait()

def main():
    #parser = argparse.ArgumentParser(os.path.basename(__file__))
    #args = parser.parse_args()

    generate()
    deploy()

if __name__ == "__main__":
    main()

