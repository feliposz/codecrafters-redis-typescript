import * as path from "jsr:@std/path";
import * as base64 from "jsr:@std/encoding/base64";
import { iterateReader } from "@std/io/iterate-reader";

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
  connection: Deno.TcpConn;
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
        i++;
        break;
      case "--replicaof":
        cfg.role = "slave";
        cfg.replicaOfHost = Deno.args[i + 1];
        cfg.replicaOfPort = parseInt(Deno.args[i + 2], 10);
        i += 2;
        break;
    }
  }

  const kvStore = loadRdb(cfg);

  await replicaHandshake(cfg, kvStore);

  const listener = Deno.listen({
    hostname: "127.0.0.1",
    port: cfg.port,
    transport: "tcp",
  });

  console.log(`listening on ${listener.addr.hostname}:${listener.addr.port}`);

  for await (const connection of listener) {
    handleConnection(connection, cfg, kvStore, true);
  }
}

async function handleConnection(
  connection: Deno.TcpConn,
  cfg: serverConfig,
  kvStore: keyValueStore,
  sendReply: boolean,
) {
  console.log("client connected");
  for await (const data of iterateReader(connection)) {
    const commands = decodeCommands(data);
    for (const cmd of commands) {
      console.log(cmd);

      switch (cmd[0].toUpperCase()) {
        case "PING":
          if (sendReply) {
            await connection.write(encodeSimple("PONG"));
          }
          break;

        case "ECHO":
          await connection.write(encodeBulk(cmd[1]));
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
            await connection.write(encodeSimple("OK"));
            propagate(cfg, cmd);
          }
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
          await connection.write(
            encodeBulk(
              `role:${cfg.role}\r\nmaster_replid:${cfg.replid}\r\nmaster_repl_offset:${cfg.offset}`,
            ),
          );
          break;

        case "REPLCONF":
          if (cmd[1].toUpperCase() === "GETACK") {
            await connection.write(
              encodeArray(["REPLCONF", "ACK", cfg.offset.toString()]),
            );
          } else if (cmd[1].toUpperCase() === "ACK") {
            // assynchronously checked inside handleWait!
            cfg.ackCount++;
            // no reply!
          } else {
            await connection.write(encodeBulk("OK"));
          }
          break;

        case "PSYNC": {
          await connection.write(
            encodeSimple(`FULLRESYNC ${cfg.replid} 0`),
          );
          const emptyRDB = base64.decodeBase64(
            "UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog==",
          );
          await connection.write(strToBytes(`\$${emptyRDB.length}\r\n`));
          await connection.write(emptyRDB);
          cfg.replicas.push({ connection, offset: 0, active: true });
          break;
        }

        case "WAIT": {
          const count = parseInt(cmd[1], 10);
          const timeout = parseInt(cmd[2], 10);
          const ackCount = await handleWait(cfg, count, timeout);
          await connection.write(encodeInt(ackCount));
          break;
        }

        case "TYPE":
          if (Object.hasOwn(kvStore, cmd[1])) {
            const entry = kvStore[cmd[1]];
            const now = new Date();
            if ((entry.expiration ?? now) < now) {
              delete kvStore[cmd[1]];
              await connection.write(encodeSimple("none"));
            } else {
              await connection.write(encodeSimple(entry.type));
            }
          } else {
            await connection.write(encodeSimple("none"));
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
          await connection.write(encodeError("command not implemented"));
      }

      // rebuild the original command to get the offset of processed commands...
      cfg.offset += encodeArray(cmd).length;
    }
  }
  console.log("client disconnected");
}

function decodeCommands(data: Uint8Array): string[][] {
  const commands: string[][] = [];
  const parts = bytesToStr(data).split("\r\n");
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

function encodeInt(x: number): Uint8Array {
  return strToBytes(`:${x}\r\n`);
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

async function replicaHandshake(cfg: serverConfig, kvStore: keyValueStore) {
  if (cfg.role === "master") {
    return;
  }

  const connection = await Deno.connect({
    hostname: cfg.replicaOfHost,
    port: cfg.replicaOfPort,
    transport: "tcp",
  });
  const buffer = new Uint8Array(1024);
  let bytesRead: number | null = 0;

  await connection.write(encodeArray(["ping"]));
  bytesRead = await connection.read(buffer);
  console.log(
    "Handshake 1 (ping): ",
    bytesToStr(buffer.slice(0, bytesRead ?? 0)),
  );

  await connection.write(
    encodeArray(["replconf", "listening-port", cfg.port.toString()]),
  );
  bytesRead = await connection.read(buffer);
  console.log(
    "Handshake 2a (listening-port): ",
    bytesToStr(buffer.slice(0, bytesRead ?? 0)),
  );

  await connection.write(encodeArray(["replconf", "capa", "psync2"]));
  bytesRead = await connection.read(buffer);
  console.log(
    "Handshake 2b (capa): ",
    bytesToStr(buffer.slice(0, bytesRead ?? 0)),
  );

  await connection.write(encodeArray(["psync", "?", "-1"]));

  // limit size of buffer to only deal with the +FULLRESYNC...
  const resyncMsgBuffer = new Uint8Array(56);
  bytesRead = await connection.read(resyncMsgBuffer);
  if (bytesRead == null) {
    throw Error("psync got no response");
  }
  console.log("Handshake 3a (psync): ", bytesToStr(resyncMsgBuffer));

  // read one byte at a time to find out the actual size of the RDB file to receive
  const byte = new Uint8Array(1);
  let rdbSize = 0;
  while (true) {
    bytesRead = await connection.read(byte);
    if ((bytesRead ?? 0) === 0) {
      throw Error("unexpected EOF from master");
    }
    if (byte[0] === 0x0a) { // LF = '\n'
      break;
    } else if (byte[0] == 0x0d) { // CR = '\r'
      continue;
    } else if (byte[0] == 0x24) { // '$'
      continue;
    } else if (byte[0] >= 0x30 && byte[0] <= 0x39) { // digits '0'-'9'
      rdbSize = rdbSize * 10 + byte[0] - 0x30;
    }
  }

  // read the RDB file exactly, leaving further messages/commands on the buffer
  const rdbBuffer = new Uint8Array(rdbSize);
  bytesRead = await connection.read(rdbBuffer);
  console.log("Handshake 3b (rdb file): bytes received", bytesRead);
  console.log(bytesToStr(rdbBuffer.slice(0, bytesRead ?? 0)));

  handleConnection(connection, cfg, kvStore, false);
}

function propagate(cfg: serverConfig, cmd: string[]) {
  cfg.replicas = cfg.replicas.filter((r) => r.active);
  for (const replica of cfg.replicas) {
    (async function (replica) {
      try {
        const bytesSent = await replica.connection.write(encodeArray(cmd));
        replica.offset += bytesSent;
      } catch {
        replica.active = false;
      }
    })(replica);
  }
}

main();

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
            const bytesSent = await replica.connection.write(
              encodeArray(["REPLCONF", "GETACK", "*"]),
            );
            replica.offset += bytesSent;
            const tmpBuffer = new Uint8Array(128);
            await replica.connection.read(tmpBuffer); // Ignoring response for now
            acknowledge(1);
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
  connection: Deno.TcpConn,
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
    await connection.write(
      encodeError("ERR The ID specified in XADD must be greater than 0-0"),
    );
  }

  if (
    timestamp < stream.last[0] ||
    (timestamp === stream.last[0] && sequence <= stream.last[1])
  ) {
    await connection.write(
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

  await connection.write(encodeBulk(`${timestamp}-${sequence}`));
}

async function handleStreamRange(
  kvStore: keyValueStore,
  connection: Deno.TcpConn,
  cmd: string[],
) {
  const streamKey = cmd[1];
  const start = cmd[2];
  const end = cmd[3];

  if (!(streamKey in kvStore)) {
    await connection.write(encodeArray([]));
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
    encodedResult += `*2\r\n\$${entry[0].length}\r\n${entry[0]}\r\n*${
      entry[1].length
    }\r\n`;
    for (const value of entry[1]) {
      encodedResult += `\$${value.length}\r\n${value}\r\n`;
    }
  }

  await connection.write(strToBytes(encodedResult));
}

async function handleStreamRead(
  kvStore: keyValueStore,
  connection: Deno.TcpConn,
  cmd: string[],
) {
  const result: [string, [string, string[]][]][] = [];

  let firstStreamArgIndex = -1;
  let firstIdArgIndex = -1;
  for (let i = 0; i < cmd.length; i++) {
    if (cmd[i].toUpperCase() === "STREAMS") {
      firstStreamArgIndex = i + 1;
      break;
    }
  }
  if (firstStreamArgIndex === -1) {
    await connection.write(encodeError("ERR No stream key argument provided"));
    return;
  }
  if ((cmd.length - firstStreamArgIndex) % 2 === 1) {
    await connection.write(encodeError("ERR Missing arguments"));
    return;
  }
  firstIdArgIndex = firstStreamArgIndex +
    (cmd.length - firstStreamArgIndex) / 2;

  const streamKey = cmd[firstStreamArgIndex];
  const start = cmd[firstIdArgIndex];

  console.log(streamKey, start);

  if (!(streamKey in kvStore)) {
    await connection.write(encodeArray([]));
    return;
  }

  const stream: streamData = kvStore[streamKey].stream!;

  const streamEntries: [string, string[]][] = [];

  let startTimestamp = 0;
  let startSequence = 0;

  startTimestamp = parseInt(start, 10);
  if (start.match(/\d+-\d+/)) {
    startSequence = parseInt(start.split("-")[1], 10);
  }

  console.log(startTimestamp, startSequence);

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

  result.push([streamKey, streamEntries]);

  console.log(result);

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

  console.log(encodedResult);

  await connection.write(strToBytes(encodedResult));
}
