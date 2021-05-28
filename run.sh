echo "Creating configuration files"

cd ourAutomation
./generateCaliperConfig.sh 

echo "Configuration files created."


echo "Deploying the network"


cd ..

export PATH=~/bin:$PATH
export VAULT_ADDR='http://130.104.229.14:30000'
export VAULT_TOKEN="s.5KkfEk3Upun8A1fQr1PfjRWL"

exec ansible-playbook  /root/hyperledger-labs/myFork/blockchain-automation-framework/platforms/shared/configuration/site.yaml -e "@/root/hyperledger-labs/myFork/blockchain-automation-framework/ourAutomation/bafNetwork.yaml" -i inventory.ini