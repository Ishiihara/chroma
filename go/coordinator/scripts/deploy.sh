make docker

kind load docker-image chroma-coordinator

kubectl create namespace chroma

helm upgrade --install chroma \
  --namespace chroma \
  --set image.repository=chroma-coordinator \
  --set image.tag=latest \
  --set image.pullPolicy=Never \
  deploy/charts/chroma-coordinator

helm uninstall chroma --namespace chroma

kubectl logs chroma-coordinator-85f6678657-cndfx -n chroma

kubectl port-forward -n chroma svc/chroma-coordinator 6649:6649
