import * as net from "node:net";

const ClientTimeout = 1000;
const ServerTimeout = 1000;

const server: net.Server = net.createServer();

server.on("connection", (connection: net.Socket) => {
  console.log("client connected");
  connection.on("close", () => {
    console.log("client disconnected");
    connection.end();
  });
  connection.on("data", (data) => {
    console.log("received data:", data);
    connection.write("+PONG\r\n");
  });
  setTimeout(() => connection.end(), ClientTimeout);
});

server.on("error", (err) => {
  throw err;
})

server.listen(6379, "127.0.0.1", () => {
  console.log("listening for connections");
});

setTimeout(() => server.close(), ServerTimeout);