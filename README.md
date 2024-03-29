# SOC and Capacity-based Scheduler (SOCCS) with DDS
This project aims to implement a dynamic scheduler to extend the lifecycle of a cluster by taking into account the State of Charge of their devices and meeting service requirements. Additionally, it incorporate a DDS module to exchange information in a multi-cluster environmment. The results of this project have been published in the paper entitled **"An Energy-Friendly Scheduler for Edge Computing Systems"** which can be found in the following link:

Article link: https://www.mdpi.com/1424-8220/21/21/7151

If you use this solution in your work, please cite it as [1] (see CITATION.cff).

## Reference
[1] Llorens-Carrodeguas, A.; G. Sagkriotis, S.; Cervelló-Pastor, C.; P. Pezaros, D. "An Energy-Friendly Scheduler for Edge Computing Systems," *Sensors* 2021, vol. 21, 7151. https://doi.org/10.3390/s21217151

# Steps to deploy a Kubernetes cluster and use SOCCS mechanism

1. Install kind to simulate a kubernetes cluster and kubectl to manage it.

--Kubectl installation--

sudo apt-get update && sudo apt-get install -y apt-transport-https gnupg2 curl

curl -s https://packages.cloud.google.com/apt/doc/apt-key.gpg | sudo apt-key add -

echo "deb https://apt.kubernetes.io/ kubernetes-xenial main" | sudo tee -a /etc/apt/sources.list.d/kubernetes.list

sudo apt-get update

sudo apt-get install -y kubectl

--Kind installation-- 

Information about Kind here: https://kind.sigs.k8s.io/docs/user/quick-start/

wget https://golang.org/dl/go1.15.3.linux-amd64.tar.gz

tar -C /usr/local -xzf go1.15.3.linux-amd64.tar.gz

export PATH=$PATH:/usr/local/go/bin

go version #to verify go installation

go get sigs.k8s.io/kind@v0.9.0

-----or-----

curl -Lo ./kind https://kind.sigs.k8s.io/dl/v0.9.0/kind-linux-amd64

chmod +x ./kind

mv ./kind /some-dir-in-your-PATH/kind

2. Go to cluster folder and run the setup script to create the cluster

sudo ./setup.sh

3. Go back to My_scheduler folder and run the deploy script

sudo ./deploy.sh 'my-scheduler'

4. Verify in the dashboard that a deployment and pod named my_scheduler have been created.

5. An option to the native kubernetes dashboard is Lens Ide. You can downloaded from this link:

https://github.com/lensapp/lens/releases/tag/v4.0.4

6. After its installation, run the command 'kubectl config view --raw' to get your kubeconfig information. Then, you can add your kubernetes cluster to Lens using that information.
