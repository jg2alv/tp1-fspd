#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
@file: fim_do_expediente.py
@description: cliente que aciona o termino do ambulatorio
@author: Augusto Guerra 
         João Vaz
@lastupdate: 2026 10 24
"""
import sys
sys.stdout.reconfigure(line_buffering=True)
import grpc
import ambulatorio_pb2 as pb
import ambulatorio_pb2_grpc as pbg
from mytimer import tstamp_msg as log

address, port = sys.argv[1], int(sys.argv[2])
log("FE(%s,%d)" % (address, port))
with grpc.insecure_channel(("[%s]:%d" if ":" in address and address[0] != "[" else "%s:%d") % (address, port)) as ch:
    log("FE -> %d" % pbg.AmbulatorioStub(ch).Termino(pb.TerminoReq()).freed)
