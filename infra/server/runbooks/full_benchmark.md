# Full Benchmark Runbook

1. Copy `infra/server/env/server.env.example` to `infra/server/env/server.env`.
2. Copy the storage env file for your target object storage.
3. Copy `infra/server/env/benchmark.env.example` to `infra/server/env/benchmark.env`.
4. Set `BENCHMARK_INPUT_DIR` to the external historical raw-data root for `2019-10` to `2020-02`.
5. Run the full benchmark:
   `bash infra/server/scripts/run-full-benchmark.sh`
6. Collect the results:
   `bash infra/server/scripts/collect-benchmark-results.sh`

Output artifacts:
- `system_benchmark.json`
- `system_benchmark.csv`
- `processing_model_benchmark.json`
- `processing_model_benchmark.csv`
- `metadata.json`

Implemented here, pending server execution:
- full historical benchmark runtime validation on server hardware
- result baselining with real cluster storage and query services
