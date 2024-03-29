#!/bin/bash

# set -e

# script creates a Kubernetes cluser from scratch
# configuration file is cluser-config.yaml


# if set to 1, create dashboard
create_dashboard=0
# if set to 1 create admin account
create_admin=1
# if set to 1 create metrics server deployment
create_metrics_server=1

# set path to kind
export PATH=$PATH:$(go env GOPATH)/

if [ "$1" != "" ]; then
    cluster_name=$1
else
    cluster_name="kind"
fi

echo $cluster_name > CLUSTERNAME

if [ "$2" != "" ]; then
    echo "Passed to many arguments"
    echo -e "usage:\n./setup.sh 'cluster name' "
    exit 1
fi

kind delete cluster --name=$cluster_name
kind create cluster --config kind-config.yml --name=$cluster_name

export KUBECONFIG="$(kind get kubeconfig-path --name=$cluster_name)"
# TODO copy new claster config file to all directories where it is required

if [ $create_dashboard == 1 ]; then
    echo "Creating dashboard..."
    kubectl apply -f https://raw.githubusercontent.com/kubernetes/dashboard/v2.0.0/aio/deploy/recommended.yaml

    # we have to wait to assign ip address to this pod TODO do while loop
    echo "Waiting for dashboard scheduling..."
    sleep 20
    # TODO make this working Error from server (NotFound): pods "kubernetes-dashboard-xyz" not found
    # pod_name=$(kubectl get pods -o wide --all-namespaces | grep "dashboard-" | awk '{printf $2}')
    # kubectl wait --for=condition=Running pod/$pod_name --all-namespaces

    # change dashboard type to NodePort
    kubectl patch svc -n kubernetes-dashboard kubernetes-dashboard --type='json' -p '[{"op":"replace","path":"/spec/type","value":"NodePort"}]'

    # address and port of node on which dashboard is running
    #node_name=$(kubectl get pods -o wide --all-namespaces | grep dashboard- | awk '{print $8}')
    #tmp_address=$(kubectl get nodes -o wide | grep "$(echo $node_name)" | awk '{print $6}')
    #tmp_port=$(kubectl -n kubernetes-dashboard get service kubernetes-dashboard  |  awk 'FNR == 2 {print $5}' | cut -d":" -f 2 | cut -d"/" -f 1)

    #echo -e "Dashboard is running at https://$tmp_address:$tmp_port"
    echo "Dashboard is installed"
    
else
    echo "Dashboard not created"
fi

if [ $create_admin == 1 ]; then
    echo "Admin account created"
    kubectl create -n kube-system serviceaccount admin
    kubectl create clusterrolebinding permissive-binding \
     --clusterrole=cluster-admin \
     --user=admin \
     --user=kubelet \
     --group=system:serviceaccounts

     admin_token_name=$(kubectl -n kube-system get serviceaccount admin -o yaml | tail -1 | cut -d":" -f 2 | cut -d" " -f2)
     admin_token=$(kubectl -n kube-system get secret $admin_token_name -o yaml | grep token | head -1 | sed -e 's/.* \(.*\)$/\1/')
     echo "Admin token: "
     echo $admin_token | base64 --decode
     echo ""
else
    echo "Admin account not created"
fi

# create metrics server-deployment
if [ $create_metrics_server == 1 ]; then
    echo "Creating metrics server deployment..."
    kubectl create -f ../metrics_server_deploy/1.8+/
    echo "Mertics server would be ready in couple minutes."
    resp=$(kubectl top nodes)
    echo "Waiting for metrics server..."
    #sleep 40
    # TODO fix this
    # resp=$(kubectl top nodes)
    while [ "$resp" = "error: metrics not available yet" ];
        do
            resp=$(kubectl top nodes)
        done
    echo "Metrics server ready!"
else
    echo "Metrics server not created"
fi

echo "To see the dashboard execute the command kubectl proxy and use the following link http://localhost:8001/api/v1/namespaces/kubernetes-dashboard/services/https:kubernetes-dashboard:/proxy/"
echo "To authenticate in the gui use the following Admin token: "
echo $admin_token | base64 --decode
echo ""

kind export kubeconfig
kubectl cluster-info
kubectl config view --raw

kubectl apply -f - <<EOF
apiVersion: v1
kind: Secret
metadata:
  name: default-token
  annotations:
    kubernetes.io/service-account.name: default
type: kubernetes.io/service-account-token
EOF

APISERVER=$(kubectl config view --minify | grep server | cut -f 2- -d ":" | tr -d " ")
SECRET_NAME=$(kubectl get secrets | grep ^default | cut -f1 -d ' ')
TOKEN=$(kubectl describe secret $SECRET_NAME | grep -E '^token' | cut -f2 -d':' | tr -d " ")

echo $TOKEN
