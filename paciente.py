#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
@file: paciente.py
@description: cliente que procura um médico no ambulatório e realiza uma consulta.
@author: Augusto Guerra
         João Vaz
@lastupdate: 2026 11 11
"""
import sys

import grpc
import ambulatorio_pb2 as apb
import ambulatorio_pb2_grpc as apbg
import consultorio_pb2 as cpb
import consultorio_pb2_grpc as cpbg
from mytimer import tstamp_msg as log


def target(address, port):
    return ("[%s]:%d" if ":" in address and not address.startswith("[")
            else "%s:%d") % (address, port)


def main():
    sys.stdout.reconfigure(line_buffering=True)
    id_pac, desc = float(sys.argv[1]), sys.argv[2]
    address, port = sys.argv[3], int(sys.argv[4])
    log("PI(%4.1f,%s)" % (id_pac, desc))
    with grpc.insecure_channel(target(address, port)) as channel:
        medico = apbg.AmbulatorioStub(channel).ProcuraMedico(apb.ProcuraMedicoReq(id_pac=id_pac))
        log("PPMR(%s:%d)" % (medico.address, medico.port))
    if medico.port == 0:
        return
    with grpc.insecure_channel(target(medico.address, medico.port)) as channel:
        resposta = cpbg.ConsultorioStub(channel).Consulta(cpb.ConsultaReq(id_pac=id_pac, desc=desc))
        log("PT -> %4.1f" % resposta.id_med)


if __name__ == "__main__":
    main()
