# First make sure your vault is deployed and unsealed, get the root token and the address of the node where the vault is running, and run:
export PATH=~/bin:$PATH
export VAULT_ADDR='http://130.104.229.14:30000'
export VAULT_TOKEN="s.5KkfEk3Upun8A1fQr1PfjRWL"



#From inside your BAF folder
cd ourAutomation
./generateCaliperConfig.sh
cd ..

# When adding new Orgs, before running the ansible-playbook command do these 2 steps manually to mount the connection profiles of the new orgs to Caliper

#   1- Go to blockchain-automation-framework/caliper/templates/job-workers.yaml   
#        and blockchain-automation-framework/caliper/templates/job-master.yaml
#     	 and add the fowlloing block for the new orgs (iinside volumeMounts)
#          - name: {{ include "caliper.fullname" . }}--yaml-config
#            mountPath: /hyperledger/caliper/workspace/buyerConnectionProfile.yaml               #here replace buyer with the name of new org
#            subPath: buyerConnectionProfile.yaml

#   2- Go to blockchain-automation-framework/caliper/templates/configmap-yamlconfigcaliper.yaml
#      and add the names of the new connection profiles in line 8



# Then inside blockchain-automation-framework/
ansible-playbook  /root/hyperledger-labs/myFork/blockchain-automation-framework/platforms/shared/configuration/site.yaml -e "@/root/hyperledger-labs/myFork/blockchain-automation-framework/ourAutomation/bafNetwork.yaml" -i inventory.ini




# Foe some reason, Caliper chart only works inside the carrier namespave (at least on my local cloud k8s so far)
kubectl config set-context --current --namespace=carrier-net
helm install caliper caliper/

# Caliper/BAF reset
helm delete caliper

#sleep(100)

ansible-playbook  /root/hyperledger-labs/myFork/blockchain-automation-framework/platforms/shared/configuration/site.yaml -e "@/root/hyperledger-labs/myFork/blockchain-automation-framework/ourAutomation/bafNetwork.yaml" -i inventory.ini -e "reset=true"

#sleep(100)





# Vault reset (not necessary for each BAF run)
helm delete vault
sleep(30)

kubectl delete pvc data-vault-0 

sleep(60)

helm repo add hashicorp https://helm.releases.hashicorp.com 

helm install vault hashicorp/vault  --set "server.dataStorage.storageClass=local-path" --set server.service.type=NodePort --set server.service.nodePort=30000 --set injector.agentImage.tag=1.0.1 --set server.image.tag=1.0.1 


kubectl exec vault-0 vault operator init >> file.yaml

#Read the file and extract the root token and unseal kyes


kubectl exec vault-0 vault operator unseal VoEyPY7ZMFiV0wFw00B3I1p4E0VlmY3u0lvrYvkTd7Jt
kubectl exec vault-0 vault operator unseal GnImcl2O/kd4WHTh4USf9UZ2GIsrJyDwwi1YcO0Lc9Pa
kubectl exec vault-0 vault operator unseal AfezdGrv0Fa6wLjFN0OvR/h2kDvQ9D0VsYXyaYjJGIMW
