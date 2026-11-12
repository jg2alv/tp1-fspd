#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
@file: ambulatorio.py
@description: servidor gRPC do ambulatório. Faz o pareamento entre pacientes e
              médicos por meio de prontuários guardados em duas filas FIFO.
@author: Augusto Guerra
         João Vaz
@lastupdate: 2026 11 11

Uso: python3 ambulatorio.py <cadeiras> <médicos> <porto>

Sincronização
-------------
Um único mutex (self.lock) protege todo o estado compartilhado: as duas filas
de prontuários, o diretório de médicos, o contador de chamadas ativas e o
indicador de fim de expediente.

Cada prontuário tem a sua própria variável de condição, criada sobre esse
mesmo mutex. Quem chega primeiro (paciente ou médico) cria o prontuário com a
sua parte das informações, coloca-o na sua fila e espera na condição do
prontuário. Quem chega depois retira o primeiro prontuário da fila do outro,
preenche a sua parte e notifica apenas a thread dona daquele prontuário.

Para o término, cada procedimento (exceto o próprio término) incrementa o
contador de chamadas ativas ao iniciar e o decrementa antes de retornar. O
término libera quem estiver esperando nas filas e depois espera, em outra
condição sobre o mesmo mutex (self.sem_ativas), até o contador chegar a zero.
Só então sinaliza o evento que faz o laço principal desligar o servidor.
"""
import sys

# Saída com buffer de linha: as mensagens chegam ao arquivo na ordem e no
# momento em que são geradas, mesmo quando a saída é redirecionada.
sys.stdout.reconfigure(line_buffering=True)

import functools
import threading
from concurrent import futures

import grpc

import ambulatorio_pb2 as pb
import ambulatorio_pb2_grpc as pbg
import mytimer
from mytimer import tstamp_msg as log

# O enunciado chama a função de my_sleep, mas o mytimer.py distribuído a chama
# de mysleep; aceitamos os dois nomes para não depender da versão do módulo.
mysleep = getattr(mytimer, "mysleep", None) or getattr(mytimer, "my_sleep")

# Tempo que o médico leva (bebendo água) antes de retornar de ChamaPaciente.
TEMPO_AGUA = 0.1


def split_peer(addr_str):
    """Extrai (endereço, porto) do string devolvido por context.peer().

    Função fornecida no enunciado, sem alterações. Para IPv6 o gRPC informa
    algo como "ipv6:%5B::1%5D:54321", e o endereço fica "%5B::1%5D", como
    nos exemplos de execução. Os clientes decodificam esse formato antes de
    abrir um canal (ver alvo() em paciente.py).
    """
    # Remove o prefixo de transporte do gRPC, se houver.
    if addr_str.startswith(("ipv4:", "ipv6:")):
        addr_str = addr_str.split(":", 1)[1]

    address, separator, port = addr_str.rpartition(":")
    # Remove os colchetes de um endereço IPv6.
    if address.startswith("[") and address.endswith("]"):
        address = address[1:-1]

    return address, int(port)


class Prontuario:
    """Registro de um par paciente/médico em formação.

    O campo da parte que ainda não chegou fica com zero. A thread que criou o
    prontuário espera em `cond` (criada sobre o mutex do ambulatório) até que
    `completo` seja verdadeiro: quando a outra parte chega ou no fim do
    expediente (nesse caso o campo da outra parte continua zero).
    """

    def __init__(self, lock, id_pac=0.0, id_med=0.0):
        self.id_pac = id_pac
        self.id_med = id_med
        self.completo = False
        self.cond = threading.Condition(lock)

    def espera(self):
        """Chamada com o mutex adquirido; wait() o libera enquanto espera."""
        while not self.completo:
            self.cond.wait()

    def libera(self):
        """Chamada com o mutex adquirido; acorda a thread dona do prontuário."""
        self.completo = True
        self.cond.notify()


def conta_chamada(metodo):
    """Mantém o contador de chamadas ativas usado pelo término.

    Incrementa ao iniciar o procedimento e decrementa depois que ele termina
    (inclusive em caso de exceção), notificando o término quando não restar
    nenhuma chamada em execução.
    """
    @functools.wraps(metodo)
    def envolvido(self, request, context):
        with self.lock:
            self.ativas += 1
        try:
            return metodo(self, request, context)
        finally:
            with self.lock:
                self.ativas -= 1
                if self.ativas == 0:
                    self.sem_ativas.notify_all()
    return envolvido


class Ambulatorio(pbg.AmbulatorioServicer):
    def __init__(self, cadeiras, num_medicos, fim):
        self.cadeiras = cadeiras          # tamanho máximo da fila de pacientes
        self.num_medicos = num_medicos    # número máximo de médicos registrados
        self.fim = fim                    # evento que encerra o servidor
        self.lock = threading.Lock()
        self.fila_pacientes = []          # prontuários de pacientes à espera
        self.fila_medicos = []            # prontuários de médicos livres
        self.diretorio = {}               # id_med -> (endereço, porto)
        self.ativas = 0                   # chamadas em execução (exceto término)
        self.sem_ativas = threading.Condition(self.lock)
        self.encerrado = False            # o término já foi chamado
        # Invariante: no máximo uma das filas tem elementos, pois um paciente
        # só espera se não houver médico livre e vice-versa.

    def _consultorio(self, id_med):
        """Endereço e porto do consultório; vazio/zero se não houver médico."""
        if id_med == 0.0:
            return "", 0
        return self.diretorio.get(id_med, ("", 0))

    @conta_chamada
    def ProcuraMedico(self, request, context):
        id_pac = request.id_pac
        with self.lock:
            log("APMI(%4.1f)" % id_pac)
            if self.encerrado:
                # Fim do expediente: não há mais atendimento.
                id_med = 0.0
            elif self.fila_medicos:
                # Há um médico livre: completa o prontuário dele e o acorda.
                pront = self.fila_medicos.pop(0)
                pront.id_pac = id_pac
                pront.libera()
                id_med = pront.id_med
            elif len(self.fila_pacientes) >= self.cadeiras:
                # Nenhum médico livre e sala de espera cheia: vai embora.
                id_med = 0.0
            else:
                # Nenhum médico livre: senta-se e espera ser chamado.
                pront = Prontuario(self.lock, id_pac=id_pac)
                self.fila_pacientes.append(pront)
                log("APME(%4.1f)" % id_pac)
                pront.espera()
                id_med = pront.id_med  # zero se liberado pelo término
            endereco, porto = self._consultorio(id_med)
            log("APMR(%4.1f) -> M(%4.1f) @ %s:%d"
                % (id_pac, id_med, endereco, porto))
        return pb.ProcuraMedicoRep(id_med=id_med, address=endereco, port=porto)

    @conta_chamada
    def RegistraMedico(self, request, context):
        # O endereço do consultório é o mesmo de onde vem esta chamada.
        endereco, _ = split_peer(context.peer())
        with self.lock:
            if len(self.diretorio) >= self.num_medicos:
                res = -1
            else:
                self.diretorio[request.id_med] = (endereco, request.port)
                res = 0
            log("ARM(%4.1f) @ %s:%d = %d"
                % (request.id_med, endereco, request.port, res))
        return pb.RegistraMedicoRep(res=res)

    @conta_chamada
    def ChamaPaciente(self, request, context):
        id_med = request.id_med
        with self.lock:
            log("ACPI(%4.1f)" % id_med)
            if self.encerrado:
                # Fim do expediente: o médico pode ir embora.
                id_pac = 0.0
            elif self.fila_pacientes:
                # Há paciente esperando: completa o prontuário dele e o acorda.
                pront = self.fila_pacientes.pop(0)
                pront.id_med = id_med
                pront.libera()
                id_pac = pront.id_pac
            else:
                # Nenhum paciente: espera na fila de médicos livres.
                pront = Prontuario(self.lock, id_med=id_med)
                self.fila_medicos.append(pront)
                log("ACPE(%4.1f)" % id_med)
                pront.espera()
                id_pac = pront.id_pac  # zero se liberado pelo término
        if id_pac != 0.0:
            # Exceto no fim do expediente, bebe água antes de atender. Fora do
            # mutex, para não atrasar as outras chamadas.
            mysleep(TEMPO_AGUA)
        with self.lock:
            log("ACPR(%4.1f) -> P(%4.1f)" % (id_med, id_pac))
        return pb.ChamaPacienteRep(id_pac=id_pac)

    def Termino(self, request, context):
        with self.lock:
            log("ATEI")
            self.encerrado = True
            # Positivo se foram liberados médicos, negativo se pacientes.
            if self.fila_medicos:
                liberados = len(self.fila_medicos)
            else:
                liberados = -len(self.fila_pacientes)
            # O campo da outra parte continua zero em cada prontuário.
            for pront in self.fila_medicos + self.fila_pacientes:
                pront.libera()
            self.fila_medicos, self.fila_pacientes = [], []
            # Espera que todas as outras chamadas, inclusive as que acabaram de
            # ser liberadas, tenham retornado.
            while self.ativas > 0:
                self.sem_ativas.wait()
            log("ATER -> %d" % liberados)
        # Não podemos encerrar o processo aqui, senão esta chamada não
        # retornaria: o evento avisa o laço principal para desligar o servidor.
        self.fim.set()
        return pb.TerminoRep(freed=liberados)


def main():
    cadeiras, num_medicos, porto = map(int, sys.argv[1:4])
    fim = threading.Event()
    # Cada paciente ou médico em espera ocupa uma thread do pool durante toda
    # a espera; sobram threads para as demais chamadas e para o término.
    pool = futures.ThreadPoolExecutor(max_workers=cadeiras + num_medicos + 32)
    # so_reuseport=0: falha se o porto já estiver em uso, em vez de dividi-lo
    # silenciosamente com outro processo.
    servidor = grpc.server(pool, options=[("grpc.so_reuseport", 0)])
    pbg.add_AmbulatorioServicer_to_server(
        Ambulatorio(cadeiras, num_medicos, fim), servidor)
    servidor.add_insecure_port("[::]:%d" % porto)
    log("AI(%d,%d)@%d" % (cadeiras, num_medicos, porto))
    servidor.start()
    # Desligamento disparado pelo término (solução sugerida no enunciado): o
    # período de graça de 1 s deixa a resposta do término chegar ao cliente.
    fim.wait()
    servidor.stop(1).wait()


if __name__ == "__main__":
    main()
