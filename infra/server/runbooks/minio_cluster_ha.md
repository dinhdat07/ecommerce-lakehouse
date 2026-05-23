# MinIO Cluster With HA Ingress

This profile deploys:
- one MinIO storage node per server
- one optional Nginx ingress proxy per selected server
- one cloud or network load balancer in front of the Nginx layer

Recommended mapping for the current project:
- `34.158.41.81`: MinIO + Nginx + control-plane services
- `34.87.25.115`: MinIO + Nginx + worker services
- `34.21.161.216`: MinIO + optional Nginx + worker services

## 1. Prepare env files

On every storage node:
1. Copy `infra/server/env/storage-minio.env.example` to `infra/server/env/storage-minio.env`.
2. Copy `infra/server/env/minio-cluster.env.example` to `infra/server/env/minio-cluster.env`.
3. Set the same `S3_ACCESS_KEY`, `S3_SECRET_KEY`, and `MINIO_CLUSTER_VOLUMES` on every node.
4. Keep `MINIO_DATA_DIR` local to the node, for example `/srv/minio/data1`.

On every Nginx ingress node:
1. Copy `infra/server/env/minio-nginx.env.example` to `infra/server/env/minio-nginx.env`.
2. Set `MINIO_NGINX_API_UPSTREAMS` to all three MinIO API endpoints.
3. Set `MINIO_NGINX_CONSOLE_UPSTREAMS` to all three console endpoints.
4. If terminating TLS at Nginx, place certs under `MINIO_NGINX_CERT_DIR` and set `MINIO_NGINX_TLS_ENABLED=true`.

## 2. Start storage nodes

Run on each MinIO node:

```bash
bash infra/server/scripts/start-minio-node.sh
```

## 3. Start ingress proxies

Run on each Nginx ingress node:

```bash
bash infra/server/scripts/start-minio-nginx.sh
```

## 4. Initialize buckets

Run once after the cluster is reachable through the stable ingress endpoint:

```bash
bash infra/server/scripts/init-minio-buckets.sh
bash infra/server/scripts/validate-minio-cluster.sh
bash infra/server/scripts/validate-storage.sh
```

## 5. Point the data plane at the ingress endpoint

Update `infra/server/env/storage-minio.env` and `infra/server/env/server.env`:
- `S3_ENDPOINT`: the cloud load balancer or Nginx VIP
- `ICEBERG_WAREHOUSE=s3://warehouse`
- bucket names: `bronze`, `silver`, `gold`, `warehouse`

Then refresh Trino:

```bash
bash infra/server/scripts/render-trino-catalog.sh
```

## 6. Recommended ingress pattern

Use Nginx only as the backend proxy layer. For a highly available public endpoint:
- place a cloud load balancer in front of the Nginx nodes
- send S3 API traffic to `MINIO_NGINX_API_PORT`
- optionally expose the MinIO console on `MINIO_NGINX_CONSOLE_PORT`
- keep MinIO API ports (`9100`, `9101`) private to the cluster network

Implemented here, pending server execution:
- multi-node MinIO startup through Docker Compose on each host
- Nginx rendering for API and console ingress
- bucket bootstrap and cluster validation scripts
- cloud load balancer creation and failover testing
