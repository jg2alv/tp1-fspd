#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
@file: fim_de_expediente.py
@description: cliente que aciona o término do ambulatório.
@author: Augusto Guerra
         João Vaz
@lastupdate: 2026 11 11

Uso: python3 fim_de_expediente.py <endereço ambulatório> <porto ambulatório>
"""
import sys

# Saída com buffer de linha: as mensagens chegam ao arquivo na ordem e no
# momento em que são geradas, mesmo quando a saída é redirecionada.
sys.stdout.reconfigure(line_buffering=True)

from urllib.parse import unquote

import grpc

import ambulatorio_pb2 as pb
import ambulatorio_pb2_grpc as pbg
from mytimer import tstamp_msg as log


def alvo(endereco, porto):
    """Monta o destino "endereço:porto" de um canal gRPC.

    Decodifica um endereço no formato devolvido pelo gRPC ("%5B::1%5D") e
    coloca colchetes em um IPv6 literal ("::1" -> "[::1]").
    """
    endereco = unquote(endereco)
    if ":" in endereco and not endereco.startswith("["):
        endereco = "[%s]" % endereco
    return "%s:%d" % (endereco, porto)


# Caso o ambulatório ainda esteja iniciando, o término usa wait_for_ready: em
# vez de falhar logo, espera até ESPERA_CONEXAO segundos que ele aceite a
# conexão, tentando de novo a cada 0,1-0,2 s (o padrão do gRPC é esperar 1 s
# ou mais entre tentativas).
ESPERA_CONEXAO = 10
OPCOES_CANAL = [("grpc.initial_reconnect_backoff_ms", 100),
                ("grpc.max_reconnect_backoff_ms", 200)]


def main():
    endereco, porto = sys.argv[1], int(sys.argv[2])
    log("FE(%s,%d)" % (endereco, porto))
    with grpc.insecure_channel(alvo(endereco, porto),
                               options=OPCOES_CANAL) as canal:
        resposta = pbg.AmbulatorioStub(canal).Termino(
            pb.TerminoReq(), wait_for_ready=True, timeout=ESPERA_CONEXAO)
        log("FE -> %d" % resposta.freed)


if __name__ == "__main__":
    main()
