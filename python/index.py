from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Query
from redis.asyncio import Redis
import asyncio
import os
import random

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")

redis: Redis | None = None

ZSET_KEY = "leaderboard:zset"
USER_SCORE_KEY = "leaderboard:user_score"
SCORE_COUNT_KEY = "leaderboard:score_count"

MIN_SCORE = 1
MAX_SCORE = 5000


@asynccontextmanager
async def lifespan(app: FastAPI):
    global redis
    redis = Redis.from_url(REDIS_URL, decode_responses=True)
    yield
    await redis.close()


app = FastAPI(title="Leaderboard Service", lifespan=lifespan)


@app.get("/ping")
async def ping():
    """
    Check if Redis is connected to FastAPI. Returns 200 if Redis responds to PING.
    """
    try:
        await redis.ping()
        return {"status": "ok", "redis": "connected"}
    except Exception as e:
        raise HTTPException(status_code=503, detail={"status": "error", "redis": "disconnected", "message": str(e)})


# ----------------------------------------------------
# Restore Random Data
# ----------------------------------------------------

@app.post("/restore_random")
async def restore_random(n: int = Query(..., description="Number of users to generate")):
    """
    Generate random leaderboard users and scores.
    """
    total = n

    pipe = redis.pipeline()

    # clear existing
    pipe.delete(ZSET_KEY)
    pipe.delete(USER_SCORE_KEY)
    pipe.delete(SCORE_COUNT_KEY)
    await pipe.execute()

    batch_size = 5000

    score_counts = {}

    for i in range(1, total + 1):

        user_id = str(i)
        score = random.randint(MIN_SCORE, MAX_SCORE)

        pipe.zadd(ZSET_KEY, {user_id: score})
        pipe.hset(USER_SCORE_KEY, user_id, score)

        score_counts[score] = score_counts.get(score, 0) + 1

        if i % batch_size == 0:
            await pipe.execute()
            pipe = redis.pipeline()

    await pipe.execute()

    # store score counts
    pipe = redis.pipeline()

    for score, count in score_counts.items():
        pipe.hset(SCORE_COUNT_KEY, score, count)

    await pipe.execute()

    return {"status": "ok", "users_loaded": total}


# ----------------------------------------------------
# Query: Score of user
# ----------------------------------------------------

@app.get("/user/{user_id}/score")
async def get_user_score(user_id: str):

    score = await redis.hget(USER_SCORE_KEY, user_id)

    if score is None:
        raise HTTPException(status_code=404, detail="User not found")

    return {
        "user_id": user_id,
        "score": int(score)
    }


# ----------------------------------------------------
# Query: Rank of user
# ----------------------------------------------------

@app.get("/user/{user_id}/rank")
async def get_user_rank(user_id: str):

    rank = await redis.zrevrank(ZSET_KEY, user_id)

    if rank is None:
        raise HTTPException(status_code=404, detail="User not found")

    return {
        "user_id": user_id,
        "rank": rank + 1
    }


# ----------------------------------------------------
# Query: count users with score X
# ----------------------------------------------------

@app.get("/score/{score}/count")
async def users_with_score(score: int):

    count = await redis.hget(SCORE_COUNT_KEY, score)

    if count is None:
        return {"score": score, "count": 0}

    return {
        "score": score,
        "count": int(count)
    }


# ----------------------------------------------------
# Query: leaderboard
# ----------------------------------------------------

@app.get("/leaderboard")
async def get_leaderboard(
    min_rank: int | None = Query(None, ge=1, description="Start rank (1-based, inclusive)"),
    max_rank: int | None = Query(None, ge=1, description="End rank (1-based, inclusive)"),
    min_score: int | None = Query(None, description="Minimum score (inclusive)"),
    max_score: int | None = Query(None, description="Maximum score (inclusive)"),
):
    """
    Get users in the given range of ranks and/or scores.
    - Use min_rank + max_rank for rank range (e.g. top 10: min_rank=1, max_rank=10).
    - Use min_score + max_score for score range.
    - If both are set, returns users that fall in the rank range AND score range.
    - If neither rank nor score range is set, returns top 100 by rank.
    """
    if min_rank is not None and max_rank is not None and min_rank > max_rank:
        raise HTTPException(status_code=400, detail="min_rank must be <= max_rank")
    if min_score is not None and max_score is not None and min_score > max_score:
        raise HTTPException(status_code=400, detail="min_score must be <= max_score")

    users_with_scores: list[tuple[str, float]] = []

    if min_rank is not None and max_rank is not None:
        # Fetch by rank range (0-based index in Redis)
        users_with_scores = await redis.zrevrange(
            ZSET_KEY,
            min_rank - 1,
            max_rank - 1,
            withscores=True,
        )
    elif min_score is not None or max_score is not None:
        # Fetch by score range (Redis: min, max are inclusive for ZRANGEBYSCORE)
        score_min = min_score if min_score is not None else "-inf"
        score_max = max_score if max_score is not None else "+inf"
        users_with_scores = await redis.zrevrangebyscore(
            ZSET_KEY,
            max=score_max,
            min=score_min,
            withscores=True,
        )
    else:
        # Default: top 100
        users_with_scores = await redis.zrevrange(
            ZSET_KEY, 0, 99, withscores=True
        )

    result = []
    if not users_with_scores:
        return {"leaderboard": result}

    # Assign ranks: for rank-based query we know the offset; for score-based we need first rank
    if min_rank is not None and max_rank is not None:
        rank_offset = min_rank
    else:
        first_user_id = users_with_scores[0][0]
        first_rank = await redis.zrevrank(ZSET_KEY, first_user_id)
        rank_offset = (first_rank + 1) if first_rank is not None else 1

    for i, (user_id, score) in enumerate(users_with_scores):
        entry = {
            "rank": rank_offset + i,
            "user_id": user_id,
            "score": int(score),
        }
        # If both rank and score filters: apply score filter in memory
        if (min_rank is not None and max_rank is not None) and (
            min_score is not None or max_score is not None
        ):
            s = int(score)
            if min_score is not None and s < min_score:
                continue
            if max_score is not None and s > max_score:
                continue
        result.append(entry)

    return {"leaderboard": result}


# ----------------------------------------------------
# Query: users in descending order
# ----------------------------------------------------

@app.get("/leaderboard/all")
async def get_all_users():

    cursor = 0
    result = []

    while True:

        cursor, data = await redis.zscan(
            ZSET_KEY,
            cursor=cursor
        )

        for user_id, score in data:
            result.append({
                "user_id": user_id,
                "score": int(score)
            })

        if cursor == 0:
            break

    result.sort(key=lambda x: x["score"], reverse=True)

    return {"users": result}
