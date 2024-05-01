import path from "path";
import { readFileSync } from "fs";
import PromiseSocket from "promise-socket"
import * as net from "net";

type keyValueStore = {
  [key: string]: {
    type: "string" | "stream";
    expiration?: Date;
    value: string;
    stream?: streamData;
  };
};

type streamData = {
  first: [number, number];
  last: [number, number];
  data: {
    [timestamp: number]: {
      [sequence: number]: string[];
    };
  };
};

type serverConfig = {
  dir: string;
  dbfilename: string;
  port: number;
  role: string;
  replicaOfHost: string;
  replicaOfPort: number;
  replid: string;
  offset: number;
  replicas: replicaState[];
  ackCount: number;
};

type replicaState = {
  connection: net.Socket;
  offset: number;
  active: boolean;
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
    replicaOfHost: "",
    replicaOfPort: 0,
    replid: genReplid(),
    offset: 0,
    replicas: [],
    ackCount: 0,
  };

  for (let i = 0; i < Bun.argv.length; i++) {
    switch (Bun.argv[i]) {
      case "--dir":
        cfg.dir = Bun.argv[i + 1];
        i++;
        break;
      case "--dbfilename":
        cfg.dbfilename = Bun.argv[i + 1];
        i++;
        break;
      case "--port":
        cfg.port = parseInt(Bun.argv[i + 1], 10);
        i++;
        break;
      case "--replicaof":
        cfg.role = "slave";
        cfg.replicaOfHost = Bun.argv[i + 1];
        cfg.replicaOfPort = parseInt(Bun.argv[i + 2], 10);
        i += 2;
        break;
    }
  }

  const kvStore = await loadRdb(cfg);

  await replicaHandshake(cfg, kvStore);

  const server: net.Server = net.createServer();
  server.listen(cfg.port, "127.0.0.1");
  console.log(`listening on 127.0.0.1:${cfg.port}`);

  server.on("connection", (connection: net.Socket) => {
    handleConnection(connection, cfg, kvStore, true);
  });


}

async function handleConnection(
  connection: net.Socket,
  cfg: serverConfig,
  kvStore: keyValueStore,
  sendReply: boolean,
) {
  console.log("client connected");

  connection.on("data", async (data) => {
    const commands = decodeCommands(data);
    for (const cmd of commands) {
      console.log(cmd);

      switch (cmd[0].toUpperCase()) {
        case "PING":
          if (sendReply) {
            connection.write(encodeSimple("PONG"));
          }
          break;

        case "ECHO":
          connection.write(encodeBulk(cmd[1]));
          break;

        case "SET":
          kvStore[cmd[1]] = { value: cmd[2], type: "string" };
          if (cmd.length === 5 && cmd[3].toUpperCase() === "PX") {
            const durationInMs = parseInt(cmd[4], 10);
            const t = new Date();
            t.setMilliseconds(t.getMilliseconds() + durationInMs);
            kvStore[cmd[1]].expiration = t;
          }
          if (sendReply) {
            connection.write(encodeSimple("OK"));
            propagate(cfg, cmd);
          }
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
                connection.write(
                  encodeArray(["dbfilename", cfg.dbfilename]),
                );
                break;
              default:
                connection.write(encodeError("not found"));
                break;
            }
          } else {
            connection.write(encodeError("action not implemented"));
          }
          break;

        case "INFO":
          connection.write(
            encodeBulk(
              `role:${cfg.role}\r\nmaster_replid:${cfg.replid}\r\nmaster_repl_offset:${cfg.offset}`,
            ),
          );
          break;

        case "REPLCONF":
          if (cmd[1].toUpperCase() === "GETACK") {
            connection.write(
              encodeArray(["REPLCONF", "ACK", cfg.offset.toString()]),
            );
          } else if (cmd[1].toUpperCase() === "ACK") {
            // assynchronously checked inside handleWait!
            cfg.ackCount++;
            // no reply!
          } else {
            connection.write(encodeBulk("OK"));
          }
          break;

        case "PSYNC": {
          connection.write(
            encodeSimple(`FULLRESYNC ${cfg.replid} 0`),
          );
          const emptyRDB = Buffer.from(
            "UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog==",
            "base64",
          );
          connection.write(strToBytes(`\$${emptyRDB.length}\r\n`));
          connection.write(emptyRDB);
          cfg.replicas.push({ connection, offset: 0, active: true });
          break;
        }

        case "WAIT": {
          const count = parseInt(cmd[1], 10);
          const timeout = parseInt(cmd[2], 10);
          const ackCount = await handleWait(cfg, count, timeout);
          connection.write(encodeInt(ackCount));
          break;
        }

        case "TYPE":
          if (Object.hasOwn(kvStore, cmd[1])) {
            const entry = kvStore[cmd[1]];
            const now = new Date();
            if ((entry.expiration ?? now) < now) {
              delete kvStore[cmd[1]];
              connection.write(encodeSimple("none"));
            } else {
              connection.write(encodeSimple(entry.type));
            }
          } else {
            connection.write(encodeSimple("none"));
          }
          break;

        case "XADD":
          await handleStreamAdd(kvStore, connection, cmd);
          break;

        case "XRANGE":
          await handleStreamRange(kvStore, connection, cmd);
          break;

        case "XREAD":
          await handleStreamRead(kvStore, connection, cmd);
          break;

        default:
          connection.write(encodeError("command not implemented"));
      }

      // rebuild the original command to get the offset of processed commands...
      cfg.offset += encodeArray(cmd).length;
    }
  });

  connection.on("close", () => {
    console.log("client disconnected");
  })
}

function decodeCommands(data: Uint8Array): string[][] {
  const commands: string[][] = [];
  const parts = bytesToStr(data).split("\r\n");
  console.log(parts);
  let index = 0;
  while (index < parts.length) {
    if (parts[index] === "") {
      break;
    }
    const cmd: string[] = [];
    if (!parts[index].startsWith("*")) {
      throw Error("expected RESP array");
    }
    const arrSize = parseInt(parts[index++].replace("*", ""), 10);
    for (let i = 0; i < arrSize; i++) {
      if (!parts[index].startsWith("$")) {
        throw Error("expected RESP bulk string");
      }
      const strSize = parseInt(parts[index++].replace("$", ""), 10);
      const str = parts[index++];
      if (str.length != strSize) {
        throw Error("bulk string size mismatch");
      }
      cmd.push(str);
    }
    commands.push(cmd);
  }
  return commands;
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

function encodeInt(x: number): string {
  return `:${x}\r\n`;
}

function encodeError(s: string): string {
  return `-${s}\r\n`;
}

function encodeArray(arr: string[]): string {
  let result = `*${arr.length}\r\n`;
  for (const s of arr) {
    result += `\$${s.length}\r\n${s}\r\n`;
  }
  return result;
}

async function loadRdb(cfg: serverConfig): Promise<keyValueStore> {
  if (cfg.dbfilename === "") {
    return {};
  }

  const rp = new RDBParser(path.join(cfg.dir, cfg.dbfilename));
  await rp.parse();
  return rp.getEntries();
}

class RDBParser {
  path: string;
  data: Uint8Array;
  index: number = 0;
  entries: keyValueStore = {};

  constructor(path: string) {
    this.path = path;
    this.data = new Uint8Array();
  }

  async parse() {

    if (this.path === undefined) {
      return;
    }

    try {
      this.data = readFileSync(this.path);
    } catch (e) {
      console.log(`skipped reading RDB file: ${this.path}`);
      console.log(e);
      return;
    }

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
            this.entries[key] = { value, expiration, type: "string" };
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

function genReplid(): string {
  const digits = "0123456789abcdef";
  const result = [];
  for (let i = 0; i < 40; i++) {
    const digitIndex = Math.floor(Math.random() * digits.length);
    result.push(digits[digitIndex]);
  }
  return result.join("");
}

function replicaHandshake(cfg: serverConfig, kvStore: keyValueStore) {
  if (cfg.role === "master") {
    return;
  }

  const connection = net.connect(cfg.replicaOfPort,cfg.replicaOfHost);

    connection.write(encodeArray(["ping"]));
    let stage = 0;
    const onData = (data: Buffer) => {
      if (stage === 0) {
        console.log("Handshake 1 (ping): ", bytesToStr(data));
        connection.write(encodeArray(["replconf", "listening-port", cfg.port.toString()]));
        stage = 1;
      } else if (stage === 1) {
        console.log("Handshake 2a (listening-port): ", bytesToStr(data));
        connection.write(encodeArray(["replconf", "capa", "psync2"]));
        stage = 2;
      } else if (stage === 2) {
        console.log("Handshake 2b (capa): ", bytesToStr(data));
        connection.write(encodeArray(["psync", "?", "-1"]));
        stage = 3;
      } else if (stage === 3) {
        console.log("Handshake 3a (psync): ", bytesToStr(data));
        stage = 4;
      } else if (stage === 4) {
        console.log("Handshake 3b (rdb): ", bytesToStr(data));
        // NOTE: ignoring received RDB file sync
        connection.removeListener("data", onData);
        handleConnection(connection, cfg, kvStore, false);
      }
    }
    connection.addListener("data", onData);
}

function propagate(cfg: serverConfig, cmd: string[]) {
  cfg.replicas = cfg.replicas.filter((r) => r.active);
  for (const replica of cfg.replicas) {
    (async function (replica) {
      try {
        const data = encodeArray(cmd)
        replica.connection.write(data);
        replica.offset += data.length;
      } catch {
        replica.active = false;
      }
    })(replica);
  }
}

function handleWait(
  cfg: serverConfig,
  count: number,
  timeout: number,
): Promise<number> {
  cfg.ackCount = 0;
  cfg.replicas = cfg.replicas.filter((r) => r.active);
  return new Promise((resolve) => {
    const timer = setTimeout(() => {
      console.log("timeout! count: ", cfg.ackCount);
      resolve(cfg.ackCount);
    }, timeout);

    const acknowledge = (increment: number) => {
      cfg.ackCount += increment;
      console.log("acknowledged: ", cfg.ackCount);
      if (cfg.ackCount >= count) {
        console.log("wait complete!");
        clearTimeout(timer);
        resolve(cfg.ackCount);
      }
    };

    for (const replica of cfg.replicas) {
      if (replica.offset > 0) {
        (async function (replica) {
          try {
            console.log("probing replica with offset: ", replica.offset);
            const data = encodeArray(["REPLCONF", "GETACK", "*"]);
            replica.connection.write(data);
            replica.offset += data.length;
            // response comes on the command handling loop?
            // const tmpBuffer = replica.connection.read(128); // Ignoring response for now
            // acknowledge(1);
          } catch {
            replica.active = false;
            acknowledge(0);
          }
        })(replica);
      } else {
        cfg.ackCount++;
      }
    }

    acknowledge(0);
  });
}

async function handleStreamAdd(
  kvStore: keyValueStore,
  connection: net.Socket,
  cmd: string[],
) {
  const streamKey = cmd[1];
  const id = cmd[2];

  if (!(streamKey in kvStore)) {
    kvStore[streamKey] = {
      type: "stream",
      value: "",
      stream: { first: [0, 0], last: [0, 0], data: {} },
    };
  }

  const stream: streamData = kvStore[streamKey].stream!;

  const idParts = id.split("-");
  let timestamp = 0;
  let sequence = 0;

  if (idParts.length === 1 && idParts[0] === "*") {
    timestamp = Date.now();
  } else {
    timestamp = parseInt(idParts[0], 10);
  }

  if (idParts.length === 2) {
    if (idParts[1] === "*") {
      if (stream.last[0] === timestamp) {
        sequence = stream.last[1] + 1;
      }
    } else {
      sequence = parseInt(idParts[1], 10);
    }
  }

  if (timestamp === 0 && sequence === 0) {
    connection.write(
      encodeError("ERR The ID specified in XADD must be greater than 0-0"),
    );
  }

  if (
    timestamp < stream.last[0] ||
    (timestamp === stream.last[0] && sequence <= stream.last[1])
  ) {
    connection.write(
      encodeError(
        "ERR The ID specified in XADD is equal or smaller than the target stream top item",
      ),
    );
    return;
  }

  stream.last[0] = timestamp;
  stream.last[1] = sequence;

  if (!(timestamp in stream.data)) {
    stream.data[timestamp] = {};
  }
  stream.data[timestamp][sequence] = cmd.slice(3);

  connection.write(encodeBulk(`${timestamp}-${sequence}`));
}

async function handleStreamRange(
  kvStore: keyValueStore,
  connection: net.Socket,
  cmd: string[],
) {
  const streamKey = cmd[1];
  const start = cmd[2];
  const end = cmd[3];

  if (!(streamKey in kvStore)) {
    connection.write(encodeArray([]));
  }

  const stream: streamData = kvStore[streamKey].stream!;

  let startTimestamp = 0;
  let endTimestamp = Infinity;
  let startSequence = 0;
  let endSequence = Infinity;

  if (start === "-") {
    startTimestamp = stream.first[0];
    startSequence = stream.first[1];
  } else {
    startTimestamp = parseInt(start, 10);
    if (start.match(/\d+-\d+/)) {
      startSequence = parseInt(start.split("-")[1], 10);
    }
  }

  if (end === "+") {
    endTimestamp = stream.last[0];
    endSequence = stream.last[1];
  } else {
    endTimestamp = parseInt(end, 10);
    if (end.match(/\d+-\d+/)) {
      endSequence = parseInt(end.split("-")[1], 10);
    }
  }

  const result: [string, string[]][] = [];

  for (
    const timestamp of Object.keys(stream.data).map((n) => parseInt(n, 10))
  ) {
    if (timestamp >= startTimestamp && timestamp <= endTimestamp) {
      for (
        const sequence of Object.keys(stream.data[timestamp]).map((n) =>
          parseInt(n, 10)
        )
      ) {
        if (
          (timestamp > startTimestamp ||
            (timestamp === startTimestamp && sequence >= startSequence)) &&
          (timestamp < endTimestamp ||
            (timestamp === endTimestamp && sequence <= endSequence))
        ) {
          result.push([
            `${timestamp}-${sequence}`,
            stream.data[timestamp][sequence],
          ]);
        }
      }
    }
  }

  let encodedResult = `*${result.length}\r\n`;
  for (const entry of result) {
    encodedResult += `*2\r\n\$${entry[0].length}\r\n${entry[0]}\r\n*${entry[1].length
      }\r\n`;
    for (const value of entry[1]) {
      encodedResult += `\$${value.length}\r\n${value}\r\n`;
    }
  }

  connection.write(strToBytes(encodedResult));
}

async function handleStreamRead(
  kvStore: keyValueStore,
  connection: net.Socket,
  cmd: string[],
) {
  const result: [string, [string, string[]][]][] = [];

  let firstStreamArgIndex = -1;
  let firstIdArgIndex = -1;
  let blockTimeout = 0;
  for (let i = 0; i < cmd.length; i++) {
    if (cmd[i].toUpperCase() === "BLOCK") {
      blockTimeout = parseInt(cmd[i + 1], 10);
      if (blockTimeout === 0) {
        blockTimeout = Infinity;
      }
      i++;
    }
    if (cmd[i].toUpperCase() === "STREAMS") {
      firstStreamArgIndex = i + 1;
      break;
    }
  }
  if (firstStreamArgIndex === -1) {
    connection.write(encodeError("ERR No stream key argument provided"));
    return;
  }
  if ((cmd.length - firstStreamArgIndex) % 2 === 1) {
    connection.write(encodeError("ERR Missing arguments"));
    return;
  }
  firstIdArgIndex = firstStreamArgIndex +
    (cmd.length - firstStreamArgIndex) / 2;

  const queries: [string, number, number][] = [];

  for (
    let streamArgIndex = firstStreamArgIndex, idArgIndex = firstIdArgIndex;
    streamArgIndex < firstIdArgIndex;
    streamArgIndex++, idArgIndex++
  ) {
    const streamKey = cmd[streamArgIndex];
    const start = cmd[idArgIndex];

    let startTimestamp = 0;
    let startSequence = 0;

    if (start === "$") {
      if (streamKey in kvStore) {
        const stream: streamData = kvStore[streamKey].stream!;
        startTimestamp = stream.last[0];
        startSequence = stream.last[1];
      }
    } else {
      startTimestamp = parseInt(start, 10);
      if (start.match(/\d+-\d+/)) {
        startSequence = parseInt(start.split("-")[1], 10);
      }
    }

    queries.push([streamKey, startTimestamp, startSequence]);
  }

  const startWait = Date.now();
  while (true) {
    for (const [streamKey, startTimestamp, startSequence] of queries) {
      if (!(streamKey in kvStore)) {
        continue;
      }

      const stream: streamData = kvStore[streamKey].stream!;
      const streamEntries: [string, string[]][] = [];

      for (
        const timestamp of Object.keys(stream.data).map((n) => parseInt(n, 10))
      ) {
        if (timestamp >= startTimestamp) {
          for (
            const sequence of Object.keys(stream.data[timestamp]).map((n) =>
              parseInt(n, 10)
            )
          ) {
            if (
              (timestamp > startTimestamp ||
                (timestamp === startTimestamp && sequence > startSequence))
            ) {
              streamEntries.push([
                `${timestamp}-${sequence}`,
                stream.data[timestamp][sequence],
              ]);
            }
          }
        }
      }

      if (streamEntries.length > 0) {
        result.push([streamKey, streamEntries]);
      }
    }

    const elapsedWait = Date.now() - startWait;
    if (result.length > 0 || elapsedWait > blockTimeout) {
      break;
    }
    await sleep(100);
  }

  console.log(result);

  if (result.length === 0) {
    connection.write(encodeNull());
    return;
  }

  let encodedResult = `*${result.length}\r\n`;
  for (const stream of result) {
    encodedResult += `*2\r\n\$${stream[0].length}\r\n${stream[0]}\r\n`;
    encodedResult += `*${stream[1].length}\r\n`;
    for (const entry of stream[1]) {
      encodedResult += `*2\r\n\$${entry[0].length}\r\n${entry[0]}\r\n`;
      encodedResult += `*${entry[1].length}\r\n`;
      for (const value of entry[1]) {
        encodedResult += `\$${value.length}\r\n${value}\r\n`;
      }
    }
  }

  connection.write(strToBytes(encodedResult));
}

function sleep(milliseconds: number) {
  return new Promise((resolve) => {
    setTimeout(resolve, milliseconds);
  });
}

main();
