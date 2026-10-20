
import asyncio
import json
import os
from aiohttp import web

players = set()

async def websocket_handler(request):
    ws = web.WebSocketResponse(heartbeat=30)
    await ws.prepare(request)

    players.add(ws)
    print("Игроков онлайн:", len(players))

    await ws.send_json({
        "type": "welcome",
        "players": len(players)
    })

    try:
        async for msg in ws:
            if msg.type == web.WSMsgType.TEXT:
                for player in list(players):
                    if player is not ws and not player.closed:
                        await player.send_str(msg.data)
            elif msg.type == web.WSMsgType.ERROR:
                break
    finally:
        players.discard(ws)
        print("Игроков онлайн:", len(players))

    return ws

async def home(request):
    return web.Response(
        text="OXIDE PIXEL MULTIPLAYER SERVER ONLINE"
    )

app = web.Application()
app.router.add_get("/", home)
app.router.add_get("/health", home)
app.router.add_get("/ws", websocket_handler)

if __name__ == "__main__":
    port = int(os.environ.get("PORT", "8080"))
    web.run_app(app, host="0.0.0.0", port=port)
