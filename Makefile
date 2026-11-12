PROTOS  = ambulatorio.proto consultorio.proto
FONTES  = ambulatorio.py medico.py paciente.py fim_de_expediente.py
ENTREGA = $(PROTOS) $(FONTES) Makefile

.PHONY: all stubs clean entrega

all: stubs

# Compila as descrições dos dois protocolos, gerando os stubs.
stubs: $(PROTOS)
	python3 -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. $(PROTOS)

# Remove os arquivos gerados pelo compilador de stubs.
clean:
	rm -f *_pb2.py *_pb2_grpc.py
	rm -rf __pycache__

# Gera o .zip com os sete arquivos da entrega sem o mytimer.py.
entrega:
	rm -f tp1.zip
	zip tp1.zip $(ENTREGA)
