from fastapi import APIRouter, HTTPException
from langgraph.checkpoint.mongodb import AsyncMongoDBSaver
from config import aclient
from Agent_Modules.custom_mongodb_checkpointer import CustomAsyncMongoDBSaver


router = APIRouter(prefix="/cp", tags =["Checkpointer Routes"])

async_memory = AsyncMongoDBSaver(
    client= aclient,
    db_name= "tsc_graph_checpointer"
)

custom_async_memory = CustomAsyncMongoDBSaver(
    client= aclient,
    db_name= "tsc_graph_checpointer",
)

db = aclient["tsc_graph_checpointer"]
checkpoint_collection = db["checkpoints_aio"]
checkpointWrites_collection = db["checkpoint_writes_aio"]

@router.get("/latest_checkpoint")
async def get_latest_checkpoint(thread_id: str, user_email: str):
    config = {
        "configurable": {
            "thread_id": thread_id,
            "user_email": user_email
        }
    }
    try:
        latest_checkpoint = await async_memory.aget(config)
        if latest_checkpoint is None:
            raise HTTPException(status_code=404, detail="Checkpoint not found")
        return latest_checkpoint
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/latest_checkpoint_tuple")
async def get_latest_checkpoint_tuple(thread_id: str, user_email:str):
    config = {
        "configurable": {
            "thread_id": thread_id,
            "user_email": user_email,
        }
    }
    try:
        latest_checkpoint_tuple =await async_memory.aget_tuple(config)
        if latest_checkpoint_tuple is None:
            raise HTTPException(status_code=404, detail="Checkpoint not found")
        return {"latest_checkpoint_tuple": latest_checkpoint_tuple}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/get_specific_checkpoint_tuple")
async def get_checkpoint_tuple(thread_id: str, checkpoint_id: str, user_email:str):
    config = {
        "configurable": {
            "thread_id": thread_id,
            "user_email": user_email,
            "checkpoint_id": checkpoint_id,
        }
    }
    try:
        latest_checkpoint_tuple = await async_memory.aget_tuple(config)
        if latest_checkpoint_tuple is None:
            raise HTTPException(status_code=404, detail="Checkpoint not found")
        return {"specific_checkpoint":latest_checkpoint_tuple}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/checkpoint_tuples")
async def get_list_checkpoint_tuples(thread_id: str, user_email: str):
    config = {
        "configurable": {
            "thread_id": thread_id,
            "user_email": user_email,
        }
    }
    try:
        checkpoint_tuples = [item async for item in async_memory.alist(config)]
        if not checkpoint_tuples:
            raise HTTPException(status_code=404, detail="Checkpoint not found")
        return checkpoint_tuples
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))



@router.get("/filtered_messages")
async def get_filtered_messages(thread_id: str, user_email: str):
    config = {
        "configurable": {
            "thread_id": thread_id,
            "user_email": user_email,
        }
    }
    try:
        checkpoint_data_list = []

        async for checkpoint_tuple in async_memory.alist(config):
            if checkpoint_tuple is None:
                continue

            checkpoint_data = checkpoint_tuple[1]
            checkpoint_data_list.append(checkpoint_data)

        checkpoint_data_list.reverse()

        return {
            "checkpoint_data": checkpoint_data_list,
            "thread_id": thread_id
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/get_recent_threads")
async def get_recent_threads():
    config = {
        "configurable": {
            "user_email": "",
        }
    }

    try:
        thread_id_map = {}
        idx = 0

        async for checkpoint_tuple in custom_async_memory.alist_all(config):
            if checkpoint_tuple is None:
                idx += 1
                continue

            third_element = checkpoint_tuple[2]
            second_element = checkpoint_tuple[1]

            if third_element is None or second_element is None:
                idx += 1
                continue

            thread_id = third_element.get("thread_id")
            user_email = third_element.get("user_email")
            ts = second_element.get("ts")

            if not thread_id or not user_email or not ts:
                idx += 1
                continue

            if thread_id not in thread_id_map:
                thread_id_map[thread_id] = []
            thread_id_map[thread_id].append((idx, checkpoint_tuple))

            if len(thread_id_map[thread_id]) > 2:
                thread_id_map[thread_id].pop(0)

            idx += 1

        result = []
        for thread_id, occurrences in thread_id_map.items():
            if len(occurrences) < 2:
                continue
            _, second_to_last_tuple = occurrences[0]
            second_element = second_to_last_tuple[1]
            third_element = second_to_last_tuple[2]

            result.append({
                "thread_id": third_element.get("thread_id"),
                "user_email": third_element.get("user_email"),
                "ts": second_element.get("ts")
            })

        if not result:
            raise HTTPException(status_code=404, detail="No valid thread entries found")

        return {"recent_threads": result}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/delete_by_thread_id")
async def delete_by_thread_id(thread_id: str):
    try:
        # Delete from checkpoints_aio
        result1 = await checkpoint_collection.delete_many({"thread_id": thread_id})
        count1 = result1.deleted_count

        # Delete from checkpoint_writes_aio
        result2 = await checkpointWrites_collection.delete_many({"thread_id": thread_id})
        count2 = result2.deleted_count

        total_deleted = count1 + count2

        if total_deleted == 0:
            raise HTTPException(status_code=404, detail=f"No documents found with thread_id '{thread_id}'")

        return {
            "message": f"{total_deleted} document(s) with thread_id '{thread_id}' have been deleted.",
            "breakdown": {
                "checkpoints": count1,
                "checkpoint_writes": count2
            }
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))