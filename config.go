package main

type Conn struct {
	Driver           string
	ConnectionString string
}

var Config struct {
	SigningKey string
	Database   Conn
}
