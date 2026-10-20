import asyncio
import json
import os
import time
import uuid
from aiohttp import web, WSMsgType

# Oxide Pixel multiplayer server.
# Keeps connected-player state and a shared event log/world snapshot in memory.
# Note: Render's free service may restart/sleep; this in-memory state is then reset.

MAX_MESSAGE_SIZE = 256 * 1024
MAX_EVENTS = 500
players = {}          # websocket -> player id
player_states = {}    # player id -> latest client state
world_state = {}      # shared object id -> object data
recent_events = []    # recent shared actions for newly connected clients
lock = asyncio.Lock()
world_owner = None  # only this connected player may publish authoritative snapshots


def safe_json_loads(raw):
    try:
        value = json.loads(raw)
        return value if isinstance(value, dict) else None
    except (json.JSONDecodeError, TypeError):
        return None


async def send_json(ws, data):
    if not ws.closed:
        await ws.send_str(json.dumps(data, separators=(",", ":")))


async def broadcast(data, exclude=None):
    payload = json.dumps(data, separators=(",", ":"))
    stale = []
    for ws in list(players):
        if ws is exclude or ws.closed:
            continue
        try:
            await ws.send_str(payload)
        except (ConnectionError, RuntimeError):
            stale.append(ws)
    for ws in stale:
        players.pop(ws, None)


async def websocket_handler(request):
    global world_owner
    ws = web.WebSocketResponse(
        heartbeat=25,
        max_msg_size=MAX_MESSAGE_SIZE,
        autoping=True,
    )
    await ws.prepare(request)

    # The client can provide its own ID in a hello message. Until then,
    # assign a temporary ID so it can receive the initial server snapshot.
    player_id = "p_" + uuid.uuid4().hex[:12]

    async with lock:
        players[ws] = player_id
        count = len(players)

    await send_json(ws, {
        "type": "welcome",
        "id": player_id,
        "players": count,
        "worldOwner": world_owner,
        "serverTime": int(time.time() * 1000),
    })

    # Send a snapshot to this client and introduce it to everyone else.
    await send_json(ws, {
        "type": "snapshot",
        "players": player_states,
        "world": list(world_state.values()),
        "worldOwner": world_owner,
        "events": recent_events[-100:],
    })
    await broadcast({
        "type": "player_join",
        "id": player_id,
        "players": count,
    }, exclude=ws)

    try:
        async for msg in ws:
            if msg.type == WSMsgType.TEXT:
                data = safe_json_loads(msg.data)
                if not data:
                    continue

                kind = data.get("type")

                if kind == "hello":
                    requested_id = data.get("id")
                    if isinstance(requested_id, str) and 1 <= len(requested_id) <= 64:
                        old_id = player_id
                        player_id = requested_id
                        async with lock:
                            players[ws] = player_id
                            if old_id != player_id and old_id in player_states:
                                player_states.pop(old_id, None)
                            if world_owner is None or world_owner not in players.values():
                                world_owner = player_id
                    await send_json(ws, {
                        "type": "welcome",
                        "id": player_id,
                        "players": len(players),
                        "worldOwner": world_owner,
                        "serverTime": int(time.time() * 1000),
                    })
                    await send_json(ws, {
                        "type": "snapshot",
                        "players": player_states,
                        "world": list(world_state.values()),
                        "worldOwner": world_owner,
                        "events": recent_events[-100:],
                    })

                elif kind == "state":
                    state = data.get("state")
                    if not isinstance(state, dict):
                        continue
                    # Store the latest public state for reconnecting clients.
                    player_states[player_id] = state
                    await broadcast({
                        "type": "state",
                        "id": player_id,
                        "state": state,
                    }, exclude=ws)

                elif kind == "world_snapshot":
                    # Only the elected world owner can publish the full state. This
                    # prevents every player's local simulation overwriting everyone else.
                    world = data.get("world")
                    if player_id != world_owner or not isinstance(world, list) or len(world) > 12000:
                        continue
                    clean = [obj for obj in world if isinstance(obj, dict) and isinstance(obj.get("type"), str)]
                    world_state.clear()
                    for index, obj in enumerate(clean):
                        world_state[str(index)] = obj
                    await broadcast({
                        "type": "world_snapshot",
                        "id": player_id,
                        "world": clean,
                        "time": int(time.time() * 1000),
                    }, exclude=ws)

                elif kind == "world_patch":
                    # Forward object-level edits from a non-owner to the owner. The
                    # owner merges them into its world and then publishes a snapshot.
                    changes = data.get("changes", [])
                    append = data.get("append", [])
                    if player_id == world_owner or not isinstance(changes, list) or not isinstance(append, list):
                        continue
                    clean_changes = []
                    for item in changes[:12000]:
                        if not isinstance(item, dict) or not isinstance(item.get("i"), int):
                            continue
                        obj = item.get("o")
                        if isinstance(obj, dict) and isinstance(obj.get("type"), str) and 0 <= item["i"] < 12000:
                            clean_changes.append({"i": item["i"], "o": obj})
                    clean_append = [obj for obj in append[:2000] if isinstance(obj, dict) and isinstance(obj.get("type"), str)]
                    for ws_peer, peer_id in list(players.items()):
                        if peer_id == world_owner and not ws_peer.closed:
                            await send_json(ws_peer, {"type": "world_patch", "id": player_id, "changes": clean_changes, "append": clean_append})
                            break

                elif kind == "world_set":
                    # Client supplies a stable object id and the latest object data.
                    obj_id = data.get("id")
                    obj = data.get("object")
                    if not isinstance(obj_id, str) or not isinstance(obj, dict):
                        continue
                    if len(obj_id) > 128:
                        continue
                    world_state[obj_id] = obj
                    event = {
                        "type": "world_set",
                        "id": obj_id,
                        "object": obj,
                        "by": player_id,
                    }
                    recent_events.append(event)
                    del recent_events[:-MAX_EVENTS]
                    await broadcast(event, exclude=ws)

                elif kind == "world_remove":
                    obj_id = data.get("id")
                    if not isinstance(obj_id, str) or len(obj_id) > 128:
                        continue
                    world_state.pop(obj_id, None)
                    event = {
                        "type": "world_remove",
                        "id": obj_id,
                        "by": player_id,
                    }
                    recent_events.append(event)
                    del recent_events[:-MAX_EVENTS]
                    await broadcast(event, exclude=ws)

                elif kind == "event":
                    # For animations/actions such as swing, shot, hit, craft, equip.
                    event_name = data.get("event")
                    payload = data.get("data", {})
                    if not isinstance(event_name, str) or len(event_name) > 64:
                        continue
                    if not isinstance(payload, dict):
                        payload = {}
                    event = {
                        "type": "event",
                        "id": player_id,
                        "event": event_name,
                        "data": payload,
                        "time": int(time.time() * 1000),
                    }
                    recent_events.append(event)
                    del recent_events[:-MAX_EVENTS]
                    await broadcast(event, exclude=ws)

                elif kind == "ping":
                    await send_json(ws, {"type": "pong", "time": data.get("time")})

            elif msg.type in (WSMsgType.ERROR, WSMsgType.CLOSE, WSMsgType.CLOSED):
                break

    finally:
        async with lock:
            players.pop(ws, None)
            player_states.pop(player_id, None)
            count = len(players)
            if world_owner == player_id:
                world_owner = next(iter(players.values()), None)
                promoted_owner = world_owner
            else:
                promoted_owner = None

        await broadcast({
            "type": "player_leave",
            "id": player_id,
            "players": count,
        })
        if promoted_owner:
            await broadcast({"type": "world_owner", "id": promoted_owner})
        print("Игроков онлайн:", count)

    return ws


async def home(request):
    return web.json_response({
        "status": "online",
        "service": "OXIDE PIXEL MULTIPLAYER SERVER",
        "players": len(players),
        "worldObjects": len(world_state),
        "worldOwner": world_owner,
    })


async def health(request):
    return web.json_response({"status": "ok"})


app = web.Application(client_max_size=MAX_MESSAGE_SIZE)
app.router.add_get("/", home)
app.router.add_get("/health", health)
app.router.add_get("/ws", websocket_handler)

if __name__ == "__main__":
    port = int(os.environ.get("PORT", "8080"))
    web.run_app(app, host="0.0.0.0", port=port)
