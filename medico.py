#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
@file: medico.py
@description: processo do médico: servidor gRPC do consultório (RPC Consulta)
              e rotina de atendimento, executada pela thread principal.
@author: Augusto Guerra
         João Vaz
@lastupdate: 2026 11 11

Uso: python3 medico.py <porto consultório> <id médico> <tempo consulta>
                       <endereço ambulatório> <porto ambulatório>

Sincronização entre a RPC Consulta e a thread principal
-------------------------------------------------------
Um mutex com uma variável de condição (Consultorio.cond) protege o estado do
consultório. A RPC Consulta pode chegar antes de ChamaPaciente retornar para
o médico, já que o ambulatório faz o médico esperar 0,1 s antes de retornar.

1. O médico "abre a porta" para o paciente recebido do ambulatório
   (porta_aberta, paciente_esperado). A RPC Consulta espera por isso, cria um
   objeto Atendimento com o identificador e a descrição do paciente e o
   entrega à thread principal (self.atendimento), que o espera.
2. O médico faz o sleep da consulta e marca o atendimento como concluído.
   A RPC Consulta espera por isso antes de retornar ao paciente.

Cada RPC espera pelo seu próprio objeto Atendimento, de modo que o médico pode
voltar ao ambulatório e iniciar outro atendimento sem afetar uma RPC anterior
que ainda não retornou.
"""
import sys

# Saída com buffer de linha: as mensagens chegam ao arquivo na ordem e no
# momento em que são geradas, mesmo quando a saída é redirecionada.
sys.stdout.reconfigure(line_buffering=True)

import threading
from concurrent import futures
from urllib.parse import unquote

import grpc

import ambulatorio_pb2 as apb
import ambulatorio_pb2_grpc as apbg
import consultorio_pb2 as cpb
import consultorio_pb2_grpc as cpbg
import mytimer
from mytimer import tstamp_msg

# O enunciado chama a função de my_sleep, mas o mytimer.py distribuído a chama
# de mysleep; aceitamos os dois nomes para não depender da versão do módulo.
mysleep = getattr(mytimer, "mysleep", None) or getattr(mytimer, "my_sleep")

# A thread principal e as threads do servidor escrevem na mesma saída, e
# print() não é atômico: este mutex impede que duas linhas se misturem.
_log_lock = threading.Lock()


def log(msg):
    with _log_lock:
        tstamp_msg(msg)


def alvo(endereco, porto):
    """Monta o destino "endereço:porto" de um canal gRPC.

    Decodifica um endereço no formato devolvido pelo gRPC ("%5B::1%5D") e
    coloca colchetes em um IPv6 literal ("::1" -> "[::1]").
    """
    endereco = unquote(endereco)
    if ":" in endereco and not endereco.startswith("["):
        endereco = "[%s]" % endereco
    return "%s:%d" % (endereco, porto)


# O ambulatório pode ainda estar iniciando quando o médico é disparado (no
# exemplo, os dois são disparados juntos). Se a conexão for recusada, o
# registro é tentado de novo a cada INTERVALO_REGISTRO s, por até
# ESPERA_CONEXAO s.
ESPERA_CONEXAO = 10
INTERVALO_REGISTRO = 0.1


def registra(end_amb, porto_amb, id_med, porto):
    """Registra o médico no ambulatório; devolve (canal, valor de retorno).

    Cada tentativa usa um canal novo, para que o gRPC tente os endereços na
    ordem do resolvedor (para localhost, IPv6 primeiro, como nos exemplos).
    Isso importa porque o endereço por onde o médico se conecta é o que o
    ambulatório registra para o consultório.
    """
    tentativas = int(ESPERA_CONEXAO / INTERVALO_REGISTRO)
    while True:
        canal = grpc.insecure_channel(alvo(end_amb, porto_amb))
        try:
            res = apbg.AmbulatorioStub(canal).RegistraMedico(
                apb.RegistraMedicoReq(id_med=id_med, port=porto)).res
            return canal, res
        except grpc.RpcError as exc:
            canal.close()
            tentativas -= 1
            if exc.code() != grpc.StatusCode.UNAVAILABLE or tentativas <= 0:
                raise
        mysleep(INTERVALO_REGISTRO)


class Atendimento:
    """Dados que a RPC Consulta entrega à thread principal do médico."""

    def __init__(self, id_pac, desc):
        self.id_pac = id_pac
        self.desc = desc
        self.concluido = False  # o médico terminou a consulta


class Consultorio(cpbg.ConsultorioServicer):
    def __init__(self, id_med):
        self.id_med = id_med
        self.cond = threading.Condition(threading.Lock())
        self.porta_aberta = False      # o médico está pronto para atender
        self.paciente_esperado = None  # paciente recebido do ambulatório
        self.atendimento = None        # entregue pela RPC ao médico

    def Consulta(self, request, context):
        log("CI(%4.1f,%s)" % (request.id_pac, request.desc))
        atendimento = Atendimento(request.id_pac, request.desc)
        with self.cond:
            # Ponto 1: espera o médico abrir a porta para este paciente.
            while not (self.porta_aberta
                       and self.paciente_esperado == request.id_pac):
                self.cond.wait()
            self.porta_aberta = False
            self.atendimento = atendimento
            self.cond.notify_all()
            # Ponto 2: espera o médico terminar a consulta.
            while not atendimento.concluido:
                self.cond.wait()
            log("CR(%4.1f,%4.1f)" % (request.id_pac, self.id_med))
        return cpb.ConsultaRep(id_med=self.id_med)

    def atende(self, id_pac, tempo):
        """Executado pela thread principal depois de chamar um paciente."""
        with self.cond:
            # Ponto 1: abre a porta e espera o paciente entregar seus dados.
            self.paciente_esperado = id_pac
            self.atendimento = None
            self.porta_aberta = True
            self.cond.notify_all()
            while self.atendimento is None:
                self.cond.wait()
            atendimento = self.atendimento
            log("MAI(%4.1f,%s)" % (atendimento.id_pac, atendimento.desc))

        # A consulta em si, fora do mutex.
        mysleep(tempo)

        with self.cond:
            # Ponto 2: libera o paciente.
            atendimento.concluido = True
            self.cond.notify_all()
            log("MAF")


def main():
    porto, id_med, tempo = int(sys.argv[1]), float(sys.argv[2]), float(sys.argv[3])
    end_amb, porto_amb = sys.argv[4], int(sys.argv[5])

    # Dispara o servidor do consultório antes de se registrar.
    consultorio = Consultorio(id_med)
    servidor = grpc.server(futures.ThreadPoolExecutor(max_workers=10),
                           options=[("grpc.so_reuseport", 0)])
    cpbg.add_ConsultorioServicer_to_server(consultorio, servidor)
    servidor.add_insecure_port("[::]:%d" % porto)
    servidor.start()

    try:
        canal, res = registra(end_amb, porto_amb, id_med, porto)
        with canal:
            log("MI(%4.1f) x %3.1f @ %d = %d" % (id_med, tempo, porto, res))
            if res != 0:
                return  # registro recusado: termina

            # Rotina diária: chama um paciente, atende, repete.
            ambulatorio = apbg.AmbulatorioStub(canal)
            while True:
                log("MCPI")
                try:
                    id_pac = ambulatorio.ChamaPaciente(
                        apb.ChamaPacienteReq(id_med=id_med)).id_pac
                except grpc.RpcError:
                    # O ambulatório encerrou enquanto este médico atendia e já
                    # não aceita chamadas: não há mais pacientes.
                    return
                log("MCPR -> %4.1f" % id_pac)
                if id_pac < 1:
                    log("MT ")  # fim do expediente
                    return
                consultorio.atende(id_pac, tempo)
    finally:
        # Encerra o servidor do consultório; o período de graça deixa a última
        # RPC Consulta enviar sua resposta.
        servidor.stop(1).wait()


if __name__ == "__main__":
    main()
