import asyncio
import json
import os
import warnings
from typing import Any, Dict, List

import elasticsearch
import streamlit as st
import tqdm.asyncio as atqdm
from elasticsearch import ElasticsearchWarning, helpers

warnings.simplefilter("ignore", category=ElasticsearchWarning)


async def migrate_index(task: Dict[str, Any]) -> None:
    """
    Migrate documents from a source Elasticsearch index to a destination index.

    Args:
        task (Dict[str, Any]): Task parameters including source and destination Elasticsearch configurations,
                               indices, query, and other settings.

    Returns:
        None
    """
    try:
        src_es = elasticsearch.AsyncElasticsearch(
            hosts=task["src_es_hosts"].split(","),
            headers={"Content-Type": "application/json"},
        )
        dst_es = elasticsearch.AsyncElasticsearch(
            hosts=task["dst_es_hosts"].split(","),
            headers={"Content-Type": "application/json"},
        )

        docs_per_request = task["docs_per_request"]

        src_index_settings = await src_es.indices.get_settings(index=task["src_index"])
        src_index_mapping = await src_es.indices.get_mapping(index=task["src_index"])

        for key in ["creation_date", "provided_name", "uuid", "version"]:
            src_index_settings[task["src_index"]]["settings"]["index"].pop(key, None)

        if not await dst_es.indices.exists(index=task["dst_index"]):
            await dst_es.indices.create(
                index=task["dst_index"],
                body={
                    "settings": src_index_settings[task["src_index"]]["settings"],
                    "mappings": src_index_mapping[task["src_index"]]["mappings"],
                },
            )

        src_index_count = await src_es.count(
            index=task["src_index"], body=task["query"]
        )
        total_docs = src_index_count["count"]

        current_doc_enum = 0

        async_scan_iterator = helpers.async_scan(
            src_es, index=task["src_index"], query=task["query"], size=docs_per_request
        )
        tqdm_bar = atqdm.tqdm(async_scan_iterator, total=total_docs)
        async for doc in tqdm_bar:
            current_doc_enum += 1
            await dst_es.index(
                index=task["dst_index"], body=doc["_source"], id=doc.get("_id", None)
            )
            task["progress_bar"].progress(
                current_doc_enum / total_docs, text=f"{current_doc_enum}/{total_docs}"
            )
            tqdm_bar.desc = f"Migrating: {task['src_index']} to: {task['dst_index']}"

        task["is_completed"] = True
    except Exception as e:
        task["error"] = str(e)
    finally:
        await src_es.close()
        await dst_es.close()


async def dump_index_to_jsonl(task: Dict[str, Any]) -> None:
    """
    Dump documents from a source Elasticsearch index to JSONL files.

    Args:
        task (Dict[str, Any]): Task parameters including source Elasticsearch configuration, index, query,
                               destination directory, and other settings.

    Returns:
        None
    """
    try:
        src_es = elasticsearch.AsyncElasticsearch(
            hosts=task["src_es_hosts"].split(","),
            headers={"Content-Type": "application/json"},
        )

        docs_per_request = task["docs_per_request"]
        max_docs_per_file = task["max_docs_per_file"]
        dst_dir = task["dst_dir"]

        src_index_count = await src_es.count(
            index=task["src_index"], body=task["query"]
        )
        total_docs = src_index_count["count"]

        current_doc_enum = 0
        current_file_num = 0

        async_scan_iterator = helpers.async_scan(
            src_es, index=task["src_index"], query=task["query"], size=docs_per_request
        )

        dump_chunk: List[dict] = []

        def save_dump_chunk() -> None:
            current_file_path = os.path.join(
                dst_dir, f"{task['src_index']}-{current_file_num}.jsonl"
            )
            with open(current_file_path, "w") as f:
                lines = [json.dumps(doc) + "\n" for doc in dump_chunk]
                f.writelines(lines)
                dump_chunk.clear()

        if not os.path.exists(dst_dir):
            os.makedirs(dst_dir)

        tqdm_bar = atqdm.tqdm(async_scan_iterator, total=total_docs)
        async for doc in tqdm_bar:
            tqdm_bar.desc = f"Dumping: {task['src_index']}"
            current_doc_enum += 1
            dump_chunk.append({"_id": doc["_id"], "_source": doc["_source"]})
            if current_doc_enum % max_docs_per_file == 0:
                current_file_num += 1
                save_dump_chunk()

            task["progress_bar"].progress(
                current_doc_enum / total_docs, text=f"{current_doc_enum}/{total_docs}"
            )

        if dump_chunk:
            current_file_num += 1
            save_dump_chunk()

        task["is_completed"] = True
    except Exception as e:
        task["error"] = str(e)
    finally:
        await src_es.close()


async def main() -> None:
    """
    Main function to handle Streamlit UI and execute migration or dump tasks.

    Returns:
        None
    """
    st.header("Elasticsearch Settings")
    src_es_hosts = st.text_input(
        "Source Elasticsearch Hosts (comma-separated)", value="http://localhost:9200"
    )

    st.header("Migration Tasks")
    tasks = []
    query_input = st.text_area(
        "Elastic custom query:",
        value='{\n    "query": {\n        "match_all": {}\n    }\n}',
        height=150,
    )

    mode_tabs = st.tabs(["Elastic to Elastic", "Elastic to JSONL"])
    with mode_tabs[0]:
        with st.form("add_migration_task"):
            dst_es_hosts = st.text_input(
                "Destination Elasticsearch Hosts (comma-separated)",
                value="http://localhost:9200",
            )
            src_index = st.text_input("Source Index")
            dst_index = st.text_input("Destination Index")
            docs_per_request = st.number_input(
                "Max Docs per request", min_value=1, value=10000
            )
            progress_bar = st.progress(0, text=dst_index)
            progress_bar.empty()
            submit_button = st.form_submit_button("Migrate index")
            if submit_button:
                try:
                    query = json.loads(query_input)
                except json.JSONDecodeError as e:
                    st.error(f"Invalid JSON query: {e}")
                    return

                task = {
                    "src_es_hosts": src_es_hosts,
                    "dst_es_hosts": dst_es_hosts,
                    "query": query,
                    "src_index": src_index,
                    "docs_per_request": docs_per_request,
                    "dst_index": dst_index,
                    "progress_bar": progress_bar,
                    "is_completed": False,
                }
                tasks.append(task)
                await migrate_index(task)
                if "error" in task:
                    st.error(f"Migration error: {task['error']}")

    with mode_tabs[1]:
        with st.form("add_dump_task"):
            src_index = st.text_input("Source Index")
            docs_per_request = st.number_input(
                "Max docs per request", min_value=1, value=10000
            )
            dst_dir = st.text_input("Destination Directory", placeholder="./dumps")
            if not dst_dir:
                dst_dir = "./dumps"

            max_docs_per_file = st.number_input(
                "Max docs per dump file", min_value=1, value=100
            )
            progress_bar = st.progress(0, text=src_index)
            progress_bar.empty()
            submit_button = st.form_submit_button("Dump index")
            if submit_button:
                try:
                    query = json.loads(query_input)
                except json.JSONDecodeError as e:
                    st.error(f"Invalid JSON query: {e}")
                    return

                task = {
                    "src_es_hosts": src_es_hosts,
                    "query": query,
                    "src_index": src_index,
                    "docs_per_request": docs_per_request,
                    "dst_dir": dst_dir,
                    "max_docs_per_file": max_docs_per_file,
                    "progress_bar": progress_bar,
                    "is_completed": False,
                }
                tasks.append(task)
                await dump_index_to_jsonl(task)
                if "error" in task:
                    st.error(f"Dumping error: {task['error']}")


if __name__ == "__main__":
    asyncio.run(main())
