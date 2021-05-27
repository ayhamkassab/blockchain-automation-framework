import sys
import yaml
import numpy as np

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
            peerCounts.append(listOfOrgs[org]["numberOfPeers"])
            orgNames.append(listOfOrgs[org]["name"])
            endorsersBooleanList.append(listOfOrgs[org]["endorser"])
        #orderersCount=(len([idx for idx in range(len(ordererOwnershipList)) if ordererOwnershipList[idx] == True]))
        orderersCount = len(np.unique(ordererOwnershipList))
        print(ordererOwnershipList, orderersCount)
        domainName = fabricConfig["domain_name"]#"svc.cluster.local"
        endorsersList=([orgNames[idx] for idx in range(len(endorsersBooleanList)) if endorsersBooleanList[idx] == True])
        print(endorsersList, endorsersBooleanList)

        BAFk8sContext = fabricConfig["BAFk8s"]["context"]
        BAFk8sConfig_file = fabricConfig["BAFk8s"]["config_file"]
        vaultUrl = fabricConfig["vault"]["url"]
        vaultRootToken = fabricConfig["vault"]["root_token"]

    except yaml.YAMLError as exc:
        print(exc)


    with open("gitops.yaml", 'r') as stream:
        try:
            gitopsConfig = yaml.safe_load(stream)

            endorsersBooleanList= []
            ordererOwnershipList= []
            ordererOwners= []
            peerCounts= []
            orgNames= []
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
            
############# shift right confcaliperNetworkConfigigtx.yaml and copy it to ../caliper folder

my_file = open("caliperNetworkConfig.yaml")
string_list = my_file.readlines()
my_file.close()


for i in xrange(1,len(string_list)):
    string_list[i] = "    " + string_list[i]

my_file = open("../caliper/caliperNetworkConfig.yaml", "w")

new_file_contents = "".join(string_list)

my_file.write(new_file_contents)
my_file.close()





############# shift right connection profile files and copy it to ../caliper folder


for org in range(len(endorsersList)):
    my_file = open("{}ConnectionProfile.yaml".format(endorsersList[org]))
    string_list = my_file.readlines()
    my_file.close()


    for i in xrange(1,len(string_list)):
       string_list[i] = "    " + string_list[i]

    my_file = open("../caliper/{}ConnectionProfile.yaml".format(endorsersList[org]), "w")

    new_file_contents = "".join(string_list)

    my_file.write(new_file_contents)
    my_file.close()
