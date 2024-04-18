import * as path from "jsr:@std/path";
import { iterateReader } from "@std/io/iterate-reader";

type keyValueStore = {
  [key: string]: {
    value: string;
    expiration?: Date;
  };
};

type serverConfig = {
  dir: string;
  dbfilename: string;
  port: number;
  role: string;
};

const encoder = new TextEncoder();
const decoder = new TextDecoder();
const strToBytes = encoder.encode.bind(encoder);
const bytesToStr = decoder.decode.bind(decoder);

async function main() {
  const cfg: serverConfig = {
    dir: "",
    dbfilename: "",
    port: 6379,
    role: "master",
  };

  for (let i = 0; i < Deno.args.length; i++) {
    switch (Deno.args[i]) {
      case "--dir":
        cfg.dir = Deno.args[i + 1];
        i++;
        break;
      case "--dbfilename":
        cfg.dbfilename = Deno.args[i + 1];
        i++;
        break;
      case "--port":
        cfg.port = parseInt(Deno.args[i + 1], 10);
        break;
    }
  }

  const kvStore = loadRdb(cfg);

  const listener = Deno.listen({
    hostname: "127.0.0.1",
    port: cfg.port,
    transport: "tcp",
  });

  for await (const connection of listener) {
    handleConnection(connection, cfg, kvStore);
  }
}

async function handleConnection(
  connection: Deno.TcpConn,
  cfg: serverConfig,
  kvStore: keyValueStore,
) {
  console.log("client connected");
  for await (const data of iterateReader(connection)) {
    const cmd = decodeResp(data);
    console.log(cmd);
    switch (cmd[0].toUpperCase()) {
      case "PING":
        await connection.write(encodeSimple("PONG"));
        break;
      case "ECHO":
        await connection.write(encodeBulk(cmd[1]));
        break;
      case "SET":
        kvStore[cmd[1]] = { value: cmd[2] };
        if (cmd.length === 5 && cmd[3].toUpperCase() === "PX") {
          const durationInMs = parseInt(cmd[4], 10);
          const t = new Date();
          t.setMilliseconds(t.getMilliseconds() + durationInMs);
          kvStore[cmd[1]].expiration = t;
        }
        await connection.write(encodeSimple("OK"));
        break;
      case "GET":
        if (Object.hasOwn(kvStore, cmd[1])) {
          const entry = kvStore[cmd[1]];
          const now = new Date();
          if ((entry.expiration ?? now) < now) {
            delete kvStore[cmd[1]];
            await connection.write(encodeNull());
          } else {
            await connection.write(encodeBulk(entry.value));
          }
        } else {
          await connection.write(encodeNull());
        }
        break;
      case "KEYS":
        await connection.write(encodeArray(Object.keys(kvStore)));
        break;
      case "CONFIG":
        if (cmd.length == 3 && cmd[1].toUpperCase() === "GET") {
          switch (cmd[2].toLowerCase()) {
            case "dir":
              await connection.write(encodeArray(["dir", cfg.dir]));
              break;
            case "dbfilename":
              await connection.write(
                encodeArray(["dbfilename", cfg.dbfilename]),
              );
              break;
            default:
              await connection.write(encodeError("not found"));
              break;
          }
        } else {
          await connection.write(encodeError("action not implemented"));
        }
        break;
      case "INFO":
        await connection.write(encodeBulk(`role:${cfg.role}`));
        break;
      default:
        await connection.write(encodeError("command not implemented"));
    }
  }
  console.log("client disconnected");
}

function decodeResp(data: Uint8Array): string[] {
  const result = [];
  const parts = bytesToStr(data).split("\r\n");
  const arrSize = parseInt(parts[0].replace("*", ""), 10);
  console.log("arrSize:", arrSize);
  console.log("parts", parts);
  for (let i = 0; i < arrSize; i++) {
    const strSize = parseInt(parts[i * 2 + 1].replace("$", ""), 10);
    const str = parts[i * 2 + 2];
    if (str.length != strSize) {
      throw Error("string size mismatch");
    }
    result.push(str);
  }
  return result;
}

function encodeSimple(s: string): Uint8Array {
  return strToBytes(`+${s}\r\n`);
}

function encodeBulk(s: string): Uint8Array {
  if (s.length === 0) {
    return encodeNull();
  }
  return strToBytes(`\$${s.length}\r\n${s}\r\n`);
}

function encodeNull(): Uint8Array {
  return strToBytes(`$-1\r\n`);
}

function encodeError(s: string): Uint8Array {
  return strToBytes(`-${s}\r\n`);
}

function encodeArray(arr: string[]): Uint8Array {
  let result = `*${arr.length}\r\n`;
  for (const s of arr) {
    result += `\$${s.length}\r\n${s}\r\n`;
  }
  return strToBytes(result);
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

    const header = bytesToStr(this.data.slice(0, 5));
    if (header !== "REDIS") {
      console.log(`not a RDB file: ${this.path}`);
      return;
    }

    const version = bytesToStr(this.data.slice(5, 9));
    console.log("Version:", version);

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
    const now = new Date();
    while (this.index < this.data.length) {
      let type = this.data[this.index++];
      let expiration: Date | undefined;

      if (type === 0xFF) {
        this.index--;
        break;
      } else if (type === 0xFC) { // Expire time in milliseconds
        const milliseconds = this.readUint64();
        expiration = new Date(Number(milliseconds));
        type = this.data[this.index++];
      } else if (type === 0xFD) { // Expire time in seconds
        const seconds = this.readUint32();
        expiration = new Date(seconds * 1000);
        type = this.data[this.index++];
      }

      const key = this.readEncodedString();
      switch (type) {
        case 0: { // string encoding
          const value = this.readEncodedString();
          console.log(key, value, expiration);
          if ((expiration ?? now) >= now) {
            this.entries[key] = { value, expiration };
          }
          break;
        }
        default:
          throw Error("type not implemented: " + type);
      }
    }
  }

  readUint32(): number {
    return (this.data[this.index++]) + (this.data[this.index++] << 8) +
      (this.data[this.index++] << 16) + (this.data[this.index++] << 24);
  }

  readUint64(): bigint {
    let result = BigInt(0);
    let shift = BigInt(0);
    for (let i = 0; i < 8; i++) {
      result += BigInt(this.data[this.index++]) << shift;
      shift += BigInt(8);
    }
    return result;
  }

  readEncodedInt(): number {
    let length = 0;
    const type = this.data[this.index] >> 6;
    switch (type) {
      case 0:
        length = this.data[this.index++];
        break;
      case 1:
        length = this.data[this.index++] & 0b00111111 |
          this.data[this.index++] << 6;
        break;
      case 2:
        this.index++;
        length = this.data[this.index++] << 24 | this.data[this.index++] << 16 |
          this.data[this.index++] << 8 | this.data[this.index++];
        break;
      case 3: {
        const bitType = this.data[this.index++] & 0b00111111;
        length = this.data[this.index++];
        if (bitType > 1) {
          length |= this.data[this.index++] << 8;
        }
        if (bitType == 2) {
          length |= this.data[this.index++] << 16 |
            this.data[this.index++] << 24;
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
    const str = bytesToStr(this.data.slice(this.index, this.index + length));
    this.index += length;
    return str;
  }

  getEntries(): keyValueStore {
    return this.entries;
  }
}

main();
