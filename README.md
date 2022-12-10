# Estuary Shuttle CID Ping Job

This is a simple job that pings the CID of the Estuary Shuttle to ensure that it is still available.

## Build
```
go build -o estuary-shuttle-cid-ping
```

## Run
```
./estuary-shuttle-cid-ping --shuttle=shuttle-5.estuary.tech --numOfWorkers=100
```

