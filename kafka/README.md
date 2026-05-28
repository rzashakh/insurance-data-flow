helm repo add redpanda https://charts.redpanda.com && helm repo update
helm install redpanda redpanda/redpanda -n data-lab -f kafka/redpanda-values.yml

helm install redpanda-connectors redpanda/connectors -n data-lab -f redpanda-connector-values.yaml --wait

helm install debezium redpanda/connectors \
  -n data-lab \
  -f debezium-values.yaml \
  --wait