kubectl create secret -n maniac-ml generic config --from-file=conf=secrets/config.json
kubectl create -f frontend.yaml