# GCP Load Balancer For MinIO Ingress

Use this when the project runs on Google Cloud VMs and Nginx is already deployed on two or more nodes with `infra/server/scripts/start-minio-nginx.sh`.

Recommended target ports:
- `9000`: S3 API
- `9001`: optional MinIO console

Recommended pattern:
1. Keep MinIO node ports (`9100`, `9101`) private.
2. Expose only the Nginx ingress ports from the storage nodes.
3. Create a cloud load balancer that forwards traffic to the Nginx instances.
4. Point `S3_ENDPOINT` at the load balancer DNS name or IP.

Minimum validation after creation:
1. `curl http://<lb-host>:9000/nginx-health`
2. `curl http://<lb-host>:9000/minio/health/live`
3. `bash infra/server/scripts/validate-storage.sh`
4. `bash infra/server/scripts/validate-minio-cluster.sh`
5. stop one Nginx node and confirm the endpoint still serves traffic

Best-practice notes:
- prefer a managed load balancer over a single VM-based reverse proxy
- terminate TLS either at the load balancer or at Nginx, but keep one stable URL for Spark and Trino
- if you expose the console, keep it behind firewall rules or VPN access

Implemented here, pending GCP execution:
- repo-side Nginx config templates and startup scripts
- server-side validation against the stable MinIO ingress endpoint
- cloud load balancer provisioning and health-check wiring
