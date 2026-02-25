import asyncio
import json

from fastmcp import Client


async def main():
    async def sampling_handler(messages, params, context):
        return "Sampled response based on messages and params"

    client = Client(
        "src/mcp_servers/mcp_rsi_server.py",
        auto_initialize=False,
        sampling_handler=sampling_handler,
    )

    async with client:
        print(f"Connected: {client.is_connected()}")

        init = await client.initialize(timeout=20.0)
        print(f"Server: {init.serverInfo.name}")

        tools = await client.list_tools()
        print("Tools:", [t.name for t in tools])

        print("\n--- health_check ---")
        health_resp = await client.call_tool("health_check", {}, timeout=5.0)
        print("Response:", health_resp)

        print("\n--- compute_rsi ---")
        resp = await client.call_tool(
            "compute_rsi",
            {
                "data_path": "data/prices",
                "symbols": ["AIR.PA"],
                "window": 14,
                "price_col": "close",
                "sample_rows": 10,
            },
            timeout=60.0,
        )

        data = resp.structured_content or resp.data
        if data is None:
            for part in resp.content:
                if getattr(part, "type", None) == "json":
                    data = part.data
                    break
                if getattr(part, "type", None) == "text":
                    try:
                        data = json.loads(part.text)
                        break
                    except Exception:
                        pass

        if data is None:
            raise RuntimeError(f"No JSON in response: {resp}")

        print("Status:", data["status"])
        print("Count:", data["count"])
        print("Columns:", data["columns"])
        print("Sample:")
        for row in data["sample"]:
            print(row)

        print("\n--- compute_rsi_prompt ---")
        prompts = await client.list_prompts()
        print("Prompts:", [p.name for p in prompts])
        prompt_resp = await client.get_prompt("compute_rsi_prompt", {"symbol": "AIR.PA"})
        print("Prompt Response:", prompt_resp)

        print("\n--- compute_rsi_sampling_test ---")
        sampling_test_resp = await client.call_tool(
            "compute_rsi_sampling_test",
            {"symbol": "AIR.PA", "data_path": "data/prices"},
            timeout=60.0,
        )
        structured = sampling_test_resp.structured_content or sampling_test_resp.data
        print("Sampling Test Response:", structured)


if __name__ == "__main__":
    asyncio.run(main())
