export JAVA_HOME=/home/aschoerk/bin/java17/jdk-17.0.2/
eval $(minikube docker-env)
pushd .
cd "$(dirname "$0")"
pwd
echo handling executor
mvn clean package -DskipTests -Dmaven.javadoc.skip=true
cd clustered-service-job-executor-spring-jobs || exit 1
pwd
echo handling jobs
kubectl delete deployment clustered-service-jobs-deployment
docker rmi -f clustered-service-jobs:latest .
docker build -t clustered-service-jobs .
kubectl apply -f jobs-deployment.yaml
kill $(netstat -tpln | grep 127.*:5006.*kubectl | grep -oE [0-9]+/kubectl | grep -o [0-9]*)
sleep 3
kubectl get pods -n default | grep -v Terminating
kubectl port-forward "$(kubectl get pods -n default | grep -v Terminating | grep -Eo clustered-service-jobs[-a-z0-9A-Z]+)" 5006:5006 &
# kubectl port-forward service/clustered-service-jobs-service 8081:8081 &
cd ..

cd clustered-service-job-executor-spring || exit 1
kubectl delete deployment clustered-service-job-executor-deployment
docker rmi -f clustered-service-job-executor:latest
docker build -t clustered-service-job-executor .
kubectl apply -f executor-deployment.yaml
kill $(netstat -tpln | grep 127.*:5005.*kubectl | grep -oE [0-9]+/kubectl | grep -o [0-9]*)
sleep 3
kubectl get pods -n default | grep -v Terminating
kubectl port-forward "$(kubectl get pods -n default | grep -v Terminating | grep -Eo clustered-service-job-executor[-a-z0-9A-Z]+)" 5005:5005 &
cd ..

echo handling ingress
kubectl apply -f ingress.yaml

popd

