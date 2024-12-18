import streamlit as st
import elasticsearch
from elasticsearch import helpers
import asyncio

# Elasticsearch settings
st.sidebar.header("Elasticsearch Settings")
src_es_hosts = st.sidebar.text_input(
    "Source Elasticsearch Hosts (comma-separated)", value="http://localhost:9200"
)
dst_es_hosts = st.sidebar.text_input(
    "Destination Elasticsearch Hosts (comma-separated)", value="http://localhost:9200"
)

# Migration tasks
st.header("Migration Tasks")
tasks = []
progress_bars = {}
completed_tasks = []


# Async migration function
async def migrate_index(task):
    src_es = elasticsearch.AsyncElasticsearch(
        hosts=src_es_hosts.split(","), headers={"Content-Type": "application/json"}
    )
    dst_es = elasticsearch.AsyncElasticsearch(
        hosts=dst_es_hosts.split(","), headers={"Content-Type": "application/json"}
    )
    progress_text = task["src_index"]
    progress_bar = st.progress(0, text=progress_text)
    progress_bars[task["src_index"]] = progress_bar

    # Get source index settings and mapping
    src_index_settings = await src_es.indices.get_settings(index=task["src_index"])
    src_index_mapping = await src_es.indices.get_mapping(index=task["src_index"])

    src_index_settings[task["src_index"]]["settings"]["index"].pop("creation_date")
    src_index_settings[task["src_index"]]["settings"]["index"].pop("provided_name")
    src_index_settings[task["src_index"]]["settings"]["index"].pop("uuid")
    src_index_settings[task["src_index"]]["settings"]["index"].pop("version")

    # Create destination index with settings and mapping
    if not await dst_es.indices.exists(index=task["dst_index"]):
        await dst_es.indices.create(
            index=task["dst_index"],
            body={
                "settings": src_index_settings[task["src_index"]]["settings"],
                "mappings": src_index_mapping[task["src_index"]]["mappings"],
            },
        )

    # Get document count
    src_index_count = await src_es.count(index=task["src_index"])
    total_docs = src_index_count["count"]

    current_doc_enum = 0
    async for doc in helpers.async_scan(
        src_es, index=task["src_index"], query={"query": {"match_all": {}}}
    ):
        current_doc_enum += 1
        await dst_es.index(
            index=task["dst_index"], body=doc["_source"], id=doc.get("_id", None)
        )
        progress_bar.progress(
            current_doc_enum / total_docs, text=f"{current_doc_enum}/{total_docs}"
        )

    progress_bar.empty()
    completed_tasks.append(task)


async def main():
    # Add task form
    with st.form("add_task"):
        src_index = st.text_input("Source Index")
        dst_index = st.text_input("Destination Index")
        submit_button = st.form_submit_button("Add Task")

        if submit_button:
            task = {"src_index": src_index, "dst_index": dst_index}
            tasks.append(task)
            await migrate_index(task)


if __name__ == "__main__":
    asyncio.run(main())
