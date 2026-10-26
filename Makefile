PROTOS = ambulatorio.proto consultorio.proto
.PHONY: all stubs clean
all: stubs
stubs: $(PROTOS)
	python3 -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. $(PROTOS)
clean:
	rm -f *_pb2.py *_pb2_grpc.py
	rm -rf __pycache__
