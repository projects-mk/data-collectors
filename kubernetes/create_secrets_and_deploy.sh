DOCKERHUB_LOGIN=$1
DOCKERHUB_TOKEN=$2
NAMESPACE=data-collectors
APP_NAME=data-collectors

kubectl delete $APP_NAME-deployment -deployment -n $NAMESPACE 
kubectl delete svc $APP_NAME-service -n $NAMESPACE

kubectl create secret docker-registry gitea-creds \
    --docker-server=registry-c6af1e1a-8717-4821-9c1f-895998bf4e9c.cytr.us \
    --docker-username=$DOCKERHUB_LOGIN \
    --docker-password=$DOCKERHUB_TOKEN \
    --namespace=$NAMESPACE \
    --dry-run=client -o yaml | kubectl apply -f -



kubectl apply -f deployment.yaml -n $NAMESPACE --wait
kubectl apply -f svc.yaml -n $NAMESPACE --wait

