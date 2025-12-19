# AsyncDispatch
AsyncDispatch A high-performance, resilient data delivery engine.
This framework provides a robust, asynchronous pipeline for streaming data from JSON/JSONL files to API endpoints. Built with asyncio and aiohttp, it replaces slow sequential syncing with a coordinated team of parallel workers capable of handling thousands of requests per minute.

Key Features
🚀 High-Concurrency: Leverages a producer-worker pattern to maximize throughput without blocking.

🛡️ Fail-Fast Safety: Includes a built-in "Circuit Breaker" that shuts down the entire process if a specific error threshold is reached.

📝 Intelligent Logging: Automatically captures failed records and API error messages into JSONL files for easy debugging and re-syncing.

🔄 Automatic Retries: Handles transient network hiccups with configurable retry logic and graceful connection management.

🧠 Memory Efficient: Streams data through queues to maintain a low memory footprint even with massive datasets.
