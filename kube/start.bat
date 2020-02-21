kubectl create secret -n  perfsonar-platform generic ps-dash-config --from-file=conf=secrets/config.json
kubectl create -f frontend.yaml