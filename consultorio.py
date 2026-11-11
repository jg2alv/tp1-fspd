#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
@file: consultorio.py
@description: servidor gRPC do consultório e rotina de atendimento do médico.
@author: Augusto Guerra
         João Vaz
@lastupdate: 2026 11 11
"""
import sys
import threading
from concurrent import futures

import grpc
import ambulatorio_pb2 as apb
import ambulatorio_pb2_grpc as apbg
import consultorio_pb2 as cpb
import consultorio_pb2_grpc as cpbg
from mytimer import tstamp_msg as log, mysleep


def target(address, port):
    return ("[%s]:%d" if ":" in address and not address.startswith("[")
            else "%s:%d") % (address, port)


class Atendimento:
    def __init__(self, id_pac, desc):
        self.id_pac, self.desc = id_pac, desc
        self.concluido = False


class ConsultorioServicer(cpbg.ConsultorioServicer):
    def __init__(self, id_med):
        self.id_med = id_med
        self.cond = threading.Condition(threading.Lock())
        self.paciente_esperado = None
        self.porta_aberta = False
        self.atendimento = None

    def Consulta(self, req, ctx):
        log("CI(%4.1f,%s)" % (req.id_pac, req.desc))
        atendimento = Atendimento(req.id_pac, req.desc)
        with self.cond:
            # A RPC pode chegar antes de ChamaPaciente retornar para o médico.
            while not (self.porta_aberta and self.paciente_esperado == req.id_pac):
                self.cond.wait()
            self.porta_aberta = False
            self.atendimento = atendimento
            self.cond.notify_all()
            # O registro pertence a esta RPC, mesmo quando o próximo atendimento inicia.
            while not atendimento.concluido:
                self.cond.wait()
            log("CR(%4.1f,%4.1f)" % (req.id_pac, self.id_med))
        return cpb.ConsultaRep(id_med=self.id_med)

    def atender(self, id_pac, tempo):
        with self.cond:
            self.paciente_esperado = id_pac
            self.atendimento = None
            self.porta_aberta = True
            self.cond.notify_all()
            while self.atendimento is None:
                self.cond.wait()
            atendimento = self.atendimento
            log("MAI(%4.1f,%s)" % (atendimento.id_pac, atendimento.desc))

        mysleep(tempo)

        with self.cond:
            atendimento.concluido = True
            self.cond.notify_all()
            log("MAF")


def main():
    sys.stdout.reconfigure(line_buffering=True)
    port, id_med, tempo = int(sys.argv[1]), float(sys.argv[2]), float(sys.argv[3])
    address, amb_port = sys.argv[4], int(sys.argv[5])
    consultorio = ConsultorioServicer(id_med)
    srv = grpc.server(futures.ThreadPoolExecutor(), options=[("grpc.so_reuseport", 0)])
    cpbg.add_ConsultorioServicer_to_server(consultorio, srv)
    srv.add_insecure_port("[::]:%d" % port)
    srv.start()
    try:
        with grpc.insecure_channel(target(address, amb_port)) as channel:
            ambulatorio = apbg.AmbulatorioStub(channel)
            res = ambulatorio.RegistraMedico(apb.RegistraMedicoReq(id_med=id_med, port=port)).res
            log("MI(%4.1f) x %3.1f @ %d = %d" % (id_med, tempo, port, res))
            if res == -1:
                return
            while True:
                log("MCPI")
                try:
                    id_pac = ambulatorio.ChamaPaciente(apb.ChamaPacienteReq(id_med=id_med)).id_pac
                except grpc.RpcError as exc:
                    # O ambulatório pode encerrar enquanto este médico atende.
                    if exc.code() == grpc.StatusCode.UNAVAILABLE:
                        return
                    raise
                log("MCPR -> %4.1f" % id_pac)
                if id_pac < 1:
                    log("MT ")
                    break
                consultorio.atender(id_pac, tempo)
    finally:
        # Deixa a última RPC enviar a resposta antes de encerrar o servidor.
        srv.stop(1).wait()


if __name__ == "__main__":
    main()
