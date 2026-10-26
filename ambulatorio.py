#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
@file: ambulatorio.py
@description: servidor gRPC de ambulatorio, faz o pareamento entre o paciente
             e o médico por meio de prontuários.
@author: Augusto Guerra
         João Vaz
@lastupdate: 2026 10 24
"""
import sys
sys.stdout.reconfigure(line_buffering=True)
import threading
from concurrent import futures
import grpc
import ambulatorio_pb2 as pb
import ambulatorio_pb2_grpc as pbg
from mytimer import tstamp_msg as log, mysleep

TIMED = 0.1

def split_peer(s):
    if s.startswith(("ipv4:", "ipv6:")): s = s.split(":", 1)[1]
    a, _, p = s.rpartition(":")
    return (a[1:-1] if a[:1] == "[" and a[-1:] == "]" else a), int(p)

class Prontuario:
    def __init__(self, lock):
        self.id_pac = self.id_med = 0.0
        self.done = False
        self.cond = threading.Condition(lock)

def counted(f):
    def w(self, req, ctx):
        with self.lock: self.active += 1
        try: return f(self, req, ctx)
        finally:
            with self.lock:
                self.active -= 1
                if not self.active: self.cond_active.notify_all()
    return w

class AmbulatorioServicer(pbg.AmbulatorioServicer):
    def __init__(self, capacity, num_med, end_event):
        self.capacity, self.num_med, self.end_event = capacity, num_med, end_event
        self.lock = threading.Lock()
        self.queue_pac, self.queue_med, self.dir = [], [], {}
        self.active, self.finished = 0, False
        self.cond_active = threading.Condition(self.lock)

    def _meet(self, mine, other, k, v, msg):  # com self.lock devolve prontuario pareado
        if other:
            p = other.pop(0); setattr(p, k, v); p.done = True; p.cond.notify()
        else:
            p = Prontuario(self.lock); setattr(p, k, v); mine.append(p); log(msg)
            while not p.done: p.cond.wait()
        return p

    @counted
    def ProcuraMedico(self, req, ctx):
        id_pac = req.id_pac
        with self.lock:
            log("APMI(%4.1f)" % id_pac)
            p = None if self.finished or len(self.queue_pac) >= self.capacity else \
                self._meet(self.queue_pac, self.queue_med, "id_pac", id_pac, "APME(%4.1f) ESPERA" % id_pac)
            id_med = p.id_med if p else 0.0
            address, port = self.dir.get(id_med, ("", 0))
            log("APMR(%4.1f) -> M(%4.1f) @ %s:%d" % (id_pac, id_med, address, port))
        return pb.ProcuraMedicoRep(address=address, port=port)

    @counted
    def RegistraMedico(self, req, ctx):
        address = split_peer(ctx.peer())[0]
        with self.lock:
            res = -1 if len(self.dir) >= self.num_med else 0
            if res == 0: self.dir[req.id_med] = (address, req.port)
            log("ARM(%4.1f) @ %s:%d = %d" % (req.id_med, address, req.port, res))
        return pb.RegistraMedicoRep(res=res)

    @counted
    def ChamaPaciente(self, req, ctx):
        id_med = req.id_med
        with self.lock:
            log("ACPI(%4.1f)" % id_med)
            p = None if self.finished else \
                self._meet(self.queue_med, self.queue_pac, "id_med", id_med, "ACPE(%4.1f)" % id_med)
            id_pac = p.id_pac if p else 0.0
        if id_pac > 0: mysleep(TIMED)
        with self.lock: log("ACPR(%4.1f) -> P(%4.1f)" % (id_med, id_pac))
        return pb.ChamaPacienteRep(id_pac=id_pac)

    def Termino(self, req, ctx):
        with self.lock:
            log("ATEI")
            self.finished = True
            n = len(self.queue_med) - len(self.queue_pac)  # so uma das queues é nao vazia
            for p in self.queue_med + self.queue_pac: p.done = True; p.cond.notify() 
            self.queue_med, self.queue_pac = [], []
            while self.active: self.cond_active.wait()
            log("ATER -> %d" % n)
        self.end_event.set()
        return pb.TerminoRep(freed=n)

if __name__ == "__main__":
    capacity, num_med, port = map(int, sys.argv[1:4])
    end = threading.Event()
    srv = grpc.server(futures.ThreadPoolExecutor(capacity + num_med + 32), options=[("grpc.so_reuseport", 0)])
    pbg.add_AmbulatorioServicer_to_server(AmbulatorioServicer(capacity, num_med, end), srv)
    srv.add_insecure_port("[::]:%d" % port)
    log("AI(%d,%d)@%d" % (capacity, num_med, port))
    srv.start(); end.wait(); srv.stop(1).wait()
