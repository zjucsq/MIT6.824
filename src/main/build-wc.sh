# go build -race -buildmode=plugin -gcflags="all=-N -l" ../mrapps/wc.go
go build -race -buildmode=plugin ../mrapps/wc.go
rm -f mr-*
