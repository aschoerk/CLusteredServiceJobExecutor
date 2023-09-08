kubectl delete deployment.apps/zookeeper --namespace minikube-kafka
cd zookeeper
docker rmi minikube-zookeeper:latest
docker build -t minikube-zookeeper .
cd ..
kubectl delete deployment.apps/kafka --namespace minikube-kafka
cd kafka
docker rmi minikube-kafka:latest
docker build -t minikube-kafka:latest .
cd ..
