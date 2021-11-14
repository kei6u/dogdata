# How to install Datadog Agent to Kubernetes Cluster

Install Datadog Operator to Kubernetes Cluster.

```shell
helm repo update
helm repo add datadog https://helm.datadoghq.com
kubectl create namespace datadog
helm install datadog-operator datadog/datadog-operator -n datadog
kubectl apply -f datadogagent.yaml
```

Install kube-state-metrics in datadog.

```shell
helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
helm repo update
helm install kube-state-metrics prometheus-community/kube-state-metrics -n datadog
```
