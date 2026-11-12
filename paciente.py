#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
@file: paciente.py
@description: cliente que procura um médico no ambulatório e realiza uma
              consulta no consultório indicado.
@author: Augusto Guerra
         João Vaz
@lastupdate: 2026 11 11

Uso: python3 paciente.py <id paciente> <problema> <endereço ambulatório>
                         <porto ambulatório>
"""
import sys

# Saída com buffer de linha: as mensagens chegam ao arquivo na ordem e no
# momento em que são geradas, mesmo quando a saída é redirecionada.
sys.stdout.reconfigure(line_buffering=True)

from urllib.parse import unquote

import grpc

import ambulatorio_pb2 as apb
import ambulatorio_pb2_grpc as apbg
import consultorio_pb2 as cpb
import consultorio_pb2_grpc as cpbg
from mytimer import tstamp_msg as log


def alvo(endereco, porto):
    """Monta o destino "endereço:porto" de um canal gRPC.

    O ambulatório devolve o endereço do consultório como o gRPC o informa em
    context.peer(); para IPv6 ele vem codificado ("%5B::1%5D"). Decodificamos
    esse formato e colocamos colchetes em um IPv6 literal ("::1" -> "[::1]").
    """
    endereco = unquote(endereco)
    if ":" in endereco and not endereco.startswith("["):
        endereco = "[%s]" % endereco
    return "%s:%d" % (endereco, porto)


# O ambulatório pode ainda estar iniciando quando este processo é disparado.
# Por isso a procura usa wait_for_ready: em vez de falhar logo, espera o
# ambulatório aceitar a conexão, tentando de novo a cada 0,1-0,2 s (o padrão
# do gRPC é esperar 1 s ou mais entre tentativas). Não há prazo, pois a
# espera por um médico na sala de espera pode ser longa. O consultório não
# precisa disso: o médico o dispara antes de se registrar no ambulatório.
OPCOES_CANAL = [("grpc.initial_reconnect_backoff_ms", 100),
                ("grpc.max_reconnect_backoff_ms", 200)]


def main():
    id_pac, desc = float(sys.argv[1]), sys.argv[2]
    end_amb, porto_amb = sys.argv[3], int(sys.argv[4])
    log("PI(%4.1f,%s)" % (id_pac, desc))

    with grpc.insecure_channel(alvo(end_amb, porto_amb),
                               options=OPCOES_CANAL) as canal:
        medico = apbg.AmbulatorioStub(canal).ProcuraMedico(
            apb.ProcuraMedicoReq(id_pac=id_pac), wait_for_ready=True)
        log("PPMR(%s:%d)" % (medico.address, medico.port))
    if medico.port == 0:
        return  # sala de espera cheia ou fim do expediente

    with grpc.insecure_channel(alvo(medico.address, medico.port)) as canal:
        resposta = cpbg.ConsultorioStub(canal).Consulta(
            cpb.ConsultaReq(id_pac=id_pac, desc=desc))
        log("PT -> %4.1f" % resposta.id_med)


if __name__ == "__main__":
    main()
