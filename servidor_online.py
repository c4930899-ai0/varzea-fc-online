#!/usr/bin/env python3
"""VÁRZEA FC V1.7: salas online privadas 1x1 via relay WebSocket.
Servidor NÃO simula a partida. O anfitrião é a autoridade da simulação.
Execução: pip install -r requirements.txt && python servidor_online.py
"""
import argparse
import asyncio
import json
import logging
import os
from http import HTTPStatus
import secrets
import string
import time
from dataclasses import dataclass
from typing import Any

from websockets.asyncio.server import serve
from websockets.exceptions import ConnectionClosed

ALFABETO = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"
SALAS: dict[str, "Sala"] = {}
VERSAO = "1.7"
MAX_SALAS = 500
LOG = logging.getLogger("varzea.online")


@dataclass
class Sala:
    codigo: str
    host: Any
    guest: Any = None
    iniciada: bool = False


def pacote(raw: Any) -> dict[str, Any] | None:
    if not isinstance(raw, str) or len(raw) > 4096:
        return None
    try:
        obj = json.loads(raw)
    except (json.JSONDecodeError, UnicodeError):
        return None
    return obj if isinstance(obj, dict) else None


async def enviar(ws: Any, obj: dict[str, Any]) -> None:
    if ws is not None:
        try:
            await ws.send(json.dumps(obj, ensure_ascii=False, separators=(",", ":")))
        except ConnectionClosed:
            pass


def numero(v: Any, limite: float) -> bool:
    # Não permitir NaN, infinito, booleanos nem valores enormes.
    return type(v) in (int, float) and (-limite <= v <= limite)


def validar_mensagem(tipo: str, msg: dict[str, Any]) -> bool:
    if tipo == "controle":
        return (numero(msg.get("x"), 1.1) and numero(msg.get("y"), 1.1)
                and type(msg.get("chute")) is bool)
    if tipo == "estado":
        for campo in ("a", "b", "bola", "vel"):
            v = msg.get(campo)
            if not isinstance(v, list) or len(v) != 2 or not all(numero(q, 4000) for q in v):
                return False
        for campo in ("dono", "ga", "gb", "restante", "gc", "gbx", "tempo_mensagem"):
            if not numero(msg.get(campo), 10000):
                return False
        return isinstance(msg.get("mensagem"), str) and len(msg["mensagem"]) <= 100
    if tipo == "fim":
        return (type(msg.get("ga")) is int and 0 <= msg["ga"] <= 100
                and type(msg.get("gb")) is int and 0 <= msg["gb"] <= 100)
    return True


async def handler(ws: Any) -> None:
    sala: Sala | None = None
    papel = ""
    ultimo_rapido = 0.0
    contagem = 0
    janela = time.monotonic()
    try:
        async for raw in ws:
            msg = pacote(raw)
            if msg is None:
                await enviar(ws, {"tipo": "erro", "mensagem": "Mensagem inválida ou muito grande."})
                continue
            tipo = msg.get("tipo")
            if not isinstance(tipo, str):
                continue
            agora = time.monotonic()
            if agora - janela > 1.0:
                janela, contagem = agora, 0
            contagem += 1
            if contagem > 90:  # evita flood de mensagens por conexão
                await ws.close(code=1008, reason="Limite de mensagens")
                break

            if sala is None:
                if tipo not in ("criar", "entrar"):
                    await enviar(ws, {"tipo": "erro", "mensagem": "Crie ou entre em uma sala primeiro."})
                    continue
                if msg.get("versao") != VERSAO:
                    await enviar(ws, {"tipo": "erro", "mensagem": "Versões diferentes do VÁRZEA FC."})
                    continue
                if tipo == "criar":
                    if len(SALAS) >= MAX_SALAS:
                        await enviar(ws, {"tipo": "erro", "mensagem": "Servidor lotado. Tente novamente."})
                        continue
                    codigo = "".join(secrets.choice(ALFABETO) for _ in range(6))
                    while codigo in SALAS:
                        codigo = "".join(secrets.choice(ALFABETO) for _ in range(6))
                    sala = Sala(codigo=codigo, host=ws)
                    SALAS[codigo] = sala
                    papel = "host"
                    await enviar(ws, {"tipo": "sala_criada", "codigo": codigo})
                    LOG.info("Sala %s criada", codigo)
                else:
                    codigo = msg.get("codigo", "")
                    if not isinstance(codigo, str) or len(codigo) != 6:
                        await enviar(ws, {"tipo": "erro", "mensagem": "Código inválido."})
                        continue
                    alvo = SALAS.get(codigo.upper())
                    if alvo is None or alvo.guest is not None or alvo.iniciada:
                        await enviar(ws, {"tipo": "erro", "mensagem": "Sala não encontrada ou ocupada."})
                        continue
                    sala, papel = alvo, "guest"
                    sala.guest = ws
                    await enviar(ws, {"tipo": "sala_entrou", "codigo": sala.codigo})
                    await enviar(sala.host, {"tipo": "amigo_entrou"})
                    LOG.info("Convidado entrou em %s", codigo)
                continue

            if tipo not in ("inicio", "controle", "estado", "fim"):
                continue
            if tipo == "inicio" and papel == "host" and sala.guest is not None and not sala.iniciada:
                sala.iniciada = True
                await enviar(sala.guest, {"tipo": "inicio"})
            elif tipo in ("controle", "estado") and sala.iniciada and sala.guest is not None:
                if (tipo == "controle" and papel != "guest") or (tipo == "estado" and papel != "host"):
                    continue
                if not validar_mensagem(tipo, msg):
                    continue
                destino = sala.host if papel == "guest" else sala.guest
                # Em média 20 Hz. Permite picos para não perder o botão CHUTAR.
                if agora - ultimo_rapido < 0.008 and tipo == "estado":
                    continue
                ultimo_rapido = agora
                await enviar(destino, msg)
            elif tipo == "fim" and papel == "host" and sala.iniciada and validar_mensagem(tipo, msg):
                sala.iniciada = False
                await enviar(sala.guest, msg)
    except ConnectionClosed:
        pass
    except Exception:
        LOG.exception("Falha ao tratar conexão")
    finally:
        if sala is not None:
            if papel == "host":
                if SALAS.get(sala.codigo) is sala:
                    SALAS.pop(sala.codigo, None)
                await enviar(sala.guest, {"tipo": "amigo_saiu"})
            elif papel == "guest":
                # Fechar a sala para evitar começar um segundo jogo com estado velho.
                if SALAS.get(sala.codigo) is sala:
                    SALAS.pop(sala.codigo, None)
                await enviar(sala.host, {"tipo": "amigo_saiu"})
                sala.guest = None


async def responder_http(conn: Any, req: Any) -> Any:
    # Render precisa de uma rota HTTP de saúde; a rota / continua WebSocket.
    if req.path in ("/health", "/healthz"):
        return conn.respond(HTTPStatus.OK, "VARZEA FC ONLINE OK\n")
    if req.path == "/" and req.headers.get("Upgrade", "").lower() != "websocket":
        return conn.respond(HTTPStatus.OK, "VÁRZEA FC ONLINE - servidor ativo\n")
    return None


async def main(host: str, port: int) -> None:
    LOG.info("Servidor VÁRZEA FC V%s | ws://%s:%d", VERSAO, host, port)
    async with serve(handler, host, port, max_size=4096, max_queue=32,
                     ping_interval=15, ping_timeout=15, compression=None,
                     process_request=responder_http):
        await asyncio.Future()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="VÁRZEA FC ONLINE — servidor de salas privadas")
    parser.add_argument("--host", default="127.0.0.1", help="IP para escutar (padrão: localhost)")
    parser.add_argument("--port", type=int, default=int(os.environ.get("PORT", "8766")))
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    try:
        asyncio.run(main(args.host, args.port))
    except KeyboardInterrupt:
        pass
