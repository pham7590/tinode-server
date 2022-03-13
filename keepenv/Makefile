

all: clean tool server

tool:
	go build -o tinode-db.exe -tags arango ../tinode-db

server:
	go build -o tinode.exe -tags arango ../server

clean: 
	rm -rf *.exe