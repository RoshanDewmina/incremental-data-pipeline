# Understand the data pipeline

Start by generating20 jobs, ingesting them and explaining the difference between60 observations and20 current job rows. Duplicate source occurrences add lineage but do not create additional jobs. Later arrival does not imply later state: sequence is the ordering authority.

Walk through the batch transaction. Why must checkpoint advancement and projection updates commit together? What happens after a process dies after five completed batches, halfway through the sixth? What changes if the source file is appended? Explain why content addressing forces replay of the prefix and why event identity makes that safe.

A first-accepted ID conflict is quarantined. The same contradictory events in reversed order may select a different authoritative payload. Explain this limitation instead of claiming convergence across contradictory history. The recomputation oracle checks derived state from accepted observations, not whether the upstream producer was truthful.

Exercises (personal mastery pending):

1. Shuffle a valid event history five times and compare final projection hashes.
2. Send a terminal event before its queued event; prove the late queued observation cannot rewind state.
3. Kill the ingestion process mid-file and show source checkpoint, committed events and final oracle equality after restart.
4. Corrupt job_latest directly in an isolated test database; show verification fails, then rebuild repairs it.
5. Add a schema1 field, then attempt schema2. Explain why one is accepted and one quarantined.
6. Change an event payload under the same ID and trace its quarantine reason/lineage.
7. Compare incremental throughput, duplicate backfill cost and query latency using the benchmark's exact input size/hardware.
8. Design a broker adapter and a partitioning strategy without implying those are implemented.

No agent-generated artifact proves the user's personal contribution, production responsibility or understanding. Resume wording and interview ownership require separate user confirmation.
