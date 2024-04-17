import * as net from "node:net";

const ClientTimeout = 1000;
const ServerTimeout = 1000;

const server: net.Server = net.createServer();

type keyValueStore = {
  [key: string]: {
    value: string;
    expiration?: Date;
  };
};

type serverConfig = {
  dir: string;
  dbfilename: string;
};

const kvStore: keyValueStore = {};
const cfg: serverConfig = {
  dir: "",
  dbfilename: "",
};

for (let i = 0; i < Deno.args.length; i++) {
  if (Deno.args[i] === "--dir") {
    cfg.dir = Deno.args[i + 1];
    i++;
  } else if (Deno.args[i] === "--dbfilename") {
    cfg.dbfilename = Deno.args[i + 1];
    i++;
  }
}

server.on("connection", (connection: net.Socket) => {
  console.log("client connected");
  connection.on("close", () => {
    console.log("client disconnected");
    connection.end();
  });
  connection.on("data", (data) => {
    const cmd = decodeResp(data.toString());
    console.log(cmd);
    switch (cmd[0].toUpperCase()) {
      case "PING":
        connection.write(encodeSimple("PONG"));
        break;
      case "ECHO":
        connection.write(encodeBulk(cmd[1]));
        break;
      case "SET":
        kvStore[cmd[1]] = { value: cmd[2] };
        if (cmd.length === 5 && cmd[3].toUpperCase() === "PX") {
          const durationInMs = parseInt(cmd[4], 10);
          const t = new Date();
          t.setMilliseconds(t.getMilliseconds() + durationInMs);
          kvStore[cmd[1]].expiration = t;
        }
        connection.write(encodeSimple("OK"));
        break;
      case "GET":
        if (Object.hasOwn(kvStore, cmd[1])) {
          const entry = kvStore[cmd[1]];
          const now = new Date();
          if ((entry.expiration ?? now) < now) {
            delete kvStore[cmd[1]];
            connection.write(encodeNull());
          } else {
            connection.write(encodeBulk(entry.value));
          }
        } else {
          connection.write(encodeNull());
        }
        break;
      case "CONFIG":
        if (cmd.length == 3 && cmd[1].toUpperCase() === "GET") {
          switch (cmd[2].toLowerCase()) {
            case "dir":
              connection.write(encodeArray(["dir", cfg.dir]));
              break;
            case "dbfilename":
              connection.write(encodeArray(["dbfilename", cfg.dbfilename]));
              break;
            default:
              connection.write(encodeError("not found"));
              break;
          }
        } else {
          connection.write(encodeError("action not implemented"));
        }
        break;
      default:
        connection.write(encodeError("command not implemented"));
    }
  });
  setTimeout(() => connection.end(), ClientTimeout);
});

server.on("error", (err) => {
  throw err;
});

server.listen(6379, "127.0.0.1", () => {
  console.log("listening for connections");
});

setTimeout(() => server.close(), ServerTimeout);

function decodeResp(s: string): string[] {
  const result = [];
  const parts = s.split("\r\n");
  const arrSize = parseInt(parts[0].replace("*", ""), 10);
  //console.log("arrSize:", arrSize);
  for (let i = 0; i < arrSize; i++) {
    const strSize = parseInt(parts[i * 2 + 1].replace("$", ""), 10);
    const str = parts[i * 2 + 2];
    //console.log("str:", strSize, str.length, str);
    result.push(str);
  }
  return result;
}

function encodeSimple(s: string): string {
  return `+${s}\r\n`;
}

function encodeBulk(s: string): string {
  if (s.length === 0) {
    return encodeNull();
  }
  return `\$${s.length}\r\n${s}\r\n`;
}

function encodeNull(): string {
  return `$-1\r\n`;
}

function encodeError(s: string): string {
  return `-${s}\r\n`;
}

function encodeArray(arr: string[]): string {
  let result = `*${arr.length}\r\n`;
  for (const value of arr) {
    result += encodeBulk(value);
  }
  return result;
}