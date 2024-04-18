import * as net from "node:net";
import * as path from "jsr:@std/path";
import * as bytes from "https://deno.land/std@0.207.0/bytes/mod.ts";

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

function main() {
  const ClientTimeout = 1000;
  const ServerTimeout = 1000;

  const server: net.Server = net.createServer();

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

  const kvStore = loadRdb(cfg);

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
        case "KEYS":
          connection.write(encodeArray(Object.keys(kvStore)));
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

}

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

function stringToBytes(s: string): Uint8Array {
  return new Uint8Array(s.split("").map((s: string) => s.charCodeAt(0)));
}

function bytesToString(arr: Uint8Array): string {
  return Array.from(arr).map(n => String.fromCharCode(n)).join("");
}

function loadRdb(cfg: serverConfig): keyValueStore {
  if (cfg.dbfilename === "") {
    return {};
  }

  const rp = new RDBParser(path.join(cfg.dir, cfg.dbfilename));
  rp.parse();
  return rp.getEntries();
}

class RDBParser {

  path: string;
  data: Uint8Array;
  index: number = 0;
  entries: keyValueStore = {};

  constructor(path: string) {
    this.path = path;
    try {
      this.data = Deno.readFileSync(this.path);
    } catch (e) {
      console.log(`skipped reading RDB file: ${this.path}`);
      console.log(e);
      this.data = new Uint8Array();
      return;
    }
  }

  parse() {
    if (this.data === undefined) {
      return;
    }

    if (!bytes.startsWith(this.data, stringToBytes("REDIS"))) {
      console.log(`not a RDB file: ${this.path}`);
      return;
    }
    console.log("Version:", bytesToString(this.data.slice(5, 9)));

    // skipping header and version
    this.index = 9;

    let eof = false;

    while (!eof && this.index < this.data.length) {
      const op = this.data[this.index++];
      switch (op) {
        case 0xFA: {
          const key = this.readEncodedString();
          switch (key) {
            case "redis-ver":
              console.log(key, this.readEncodedString());
              break;
            case "redis-bits":
              console.log(key, this.readEncodedInt());
              break;
            case "ctime":
              console.log(key, new Date(this.readEncodedInt() * 1000));
              break;
            case "used-mem":
              console.log(key, this.readEncodedInt());
              break;
            case "aof-preamble":
              console.log(key, this.readEncodedInt());
              break;
            default:
              throw Error("unknown auxiliary field");
          }
          break;
        }

        case 0xFB:
          console.log("keyspace", this.readEncodedInt());
          console.log("expires", this.readEncodedInt());
          this.readEntries();
          break;

        case 0xFE:
          console.log("db selector", this.readEncodedInt());
          break;

        case 0xFF:
          eof = true;
          break;

        default:
          throw Error("op not implemented: " + op);
      }

      if (eof) {
        break;
      }
    }

  }

  readEntries() {
    while (this.index < this.data.length) {
      let type = this.data[this.index++];
      let expiration: Date | undefined;

      if (type === 0xFF) {
        this.index--;
        break;
      } else if (type === 0xFC) { // Expire time in milliseconds
        expiration = new Date(this.readEncodedInt());
        type = this.data[this.index++];
      } else if (type === 0xFD) { // Expire time in seconds
        expiration = new Date(this.readEncodedInt() * 1000);
        type = this.data[this.index++];
      }

      const key = this.readEncodedString();
      switch (type) {
        case 0: // string encoding
          this.entries[key] = { value: this.readEncodedString(), expiration };
          break;
        default:
          throw Error("type not implemented: " + type);
      }
    }
  }

  readEncodedInt(): number {
    let length = 0;
    const type = this.data[this.index] >> 6
    switch (type) {
      case 0:
        length = this.data[this.index++];
        break;
      case 1:
        length = this.data[this.index++] & 0b00111111 | this.data[this.index++] << 6;
        break;
      case 2:
        this.index++;
        length = this.data[this.index++] << 24 | this.data[this.index++] << 16 | this.data[this.index++] << 8 | this.data[this.index++];
        break;
      case 3: {
        const bitType = this.data[this.index++] & 0b00111111;
        length = this.data[this.index++];
        if (bitType > 1) {
          length |= this.data[this.index++] << 8;
        }
        if (bitType == 2) {
          length |= this.data[this.index++] << 16 | this.data[this.index++] << 24;
        }
        if (bitType > 2) {
          throw Error("length not implemented");
        }
        break;
      }
    }
    return length;
  }

  readEncodedString(): string {
    const length = this.readEncodedInt();
    const str = bytesToString(this.data.slice(this.index, this.index + length));
    this.index += length;
    return str;
  }

  getEntries(): keyValueStore {
    return this.entries;
  }

}

main();