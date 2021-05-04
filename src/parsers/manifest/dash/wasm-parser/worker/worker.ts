/**
 * Copyright 2015 CANAL+ Group
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import {
  AttributeName,
  CustomEventType,
  IIngoingMessage,
  IParsedAttributeEvent,
  IParserWarningEvent,
  IWorkerOutgoingMessage,
  IngoingMessageType,
  OutgoingMessageType,
  TagName,
} from "./worker_types";

let timer = 0;
let readTimer = 0;

const MAX_READ_SIZE = 15e3;

// We should always be in a Worker context here.
/* eslint-disable @typescript-eslint/no-unsafe-assignment */
const worker : Worker = self as unknown as Worker;

let instance : WebAssembly.WebAssemblyInstantiatedSource | null = null;
let linearMemory : WebAssembly.Memory | null = null;

let mpdData : {
  /**
   * First not-yet read position in `mpd`, in bytes.
   * When the parser asks for new data, we start giving data from that point
   * on.
   */
  cursor : number;
  /**
   * Complete data that needs to be parsed.
   * This is either the full MPD or xlinks.
   */
  mpd : ArrayBuffer;
} | null = null;

worker.onmessage = onWorkerMessage;

function onWorkerMessage(msg : MessageEvent<IIngoingMessage>) {
  const { data } = msg;
  if (data.type === IngoingMessageType.Initialize) {
    initializeWasm(data.wasmUrl)
      .then(() => triggerMessage([ OutgoingMessageType.Initialized ]))
      .catch((err) => {
        const initErrorMsg = err instanceof Error ? err.toString() :
                                                    "Unknown initialization error";
        triggerMessage([ OutgoingMessageType.InitializationError,
                         initErrorMsg ]);
      });
  } else if (data.type === IngoingMessageType.ParseMpd) {
    try {
      parseMpd(data.mpd);
    } catch (err) {
      const parsingErrorMsg = err instanceof Error ? err.toString() :
                                                     "Unknown parsing error";
      triggerMessage([ OutgoingMessageType.MPDParsingError,
                       parsingErrorMsg ]);
      return;
    }
    triggerMessage([ OutgoingMessageType.MPDParsingFinished ]);
  } else if (data.type === IngoingMessageType.ParseXlink) {
    try {
      parseXlink(data.xlink);
    } catch (err) {
      const parsingErrorMsg = err instanceof Error ? err.toString() :
                                                     "Unknown parsing error";
      triggerMessage([ OutgoingMessageType.XLinkParsingError,
                       parsingErrorMsg ]);
      return;
    }
    triggerMessage([ OutgoingMessageType.XLinkParsingFinished ]);
  }
}

/**
 * Messages send by this worker are actually buffered to avoid making too much
 * postMessage call which might (to test) have a negative effect on performance.
 *
 * This array allows to send messages in bulk once it reached a sufficient size.
 */
const msgBuffer : IWorkerOutgoingMessage[] = [];

/**
 * A large amount of events sent by this worker are under an `ArrayBuffer`
 * format which can be fully moved to the main script (from the worker), through
 * the use of postMessage's "reportables" argument.
 *
 * This array allows to store a reference to all ArrayBuffers in the `msgBuffer`
 * array.
 */
const reportables : ArrayBuffer[] = [];

function triggerMessage(msg : IWorkerOutgoingMessage) {
  msgBuffer.push(msg);
  if (msg[0] <= 10) {
    worker.postMessage(msgBuffer, reportables);
    msgBuffer.length = 0;
    reportables.length = 0;
  } else {
    let sendNow = false;
    if (msg[0] > 20) {
      // TODO I thought TS would be smarter than this
      const payload = (msg as IParserWarningEvent |
                              IParsedAttributeEvent)[1];
      reportables.push(payload);
      sendNow = sendNow || payload.byteLength > 100;
    }
    if (sendNow) {
      worker.postMessage(msgBuffer, reportables);
      msgBuffer.length = 0;
      reportables.length = 0;
    }
  }
}

function initializeWasm(wasmUrl : string) : Promise<void> {
  const imports = {
    env: {
      memoryBase: 0,
      tableBase: 0,
      memory: new WebAssembly.Memory({ initial: 10 }),
      table: new WebAssembly.Table({ initial: 2, element: "anyfunc" }),
      onTagOpen,
      onCustomEvent,
      onAttribute,
      readNext,
      onTagClose,
    },
  };

  const fetchedWasm = fetch(wasmUrl);

  const streamingProm = typeof WebAssembly.instantiateStreaming === "function" ?
    WebAssembly.instantiateStreaming(fetchedWasm, imports) :

    // We should already have checked that Promise are supported.
    // We don't want to include a polyfill to reduce the size of the worker file.
    /* eslint-disable no-restricted-properties */
    Promise.reject("`WebAssembly.instantiateStreaming` API not available");
    /* eslint-enable no-restricted-properties */

  return streamingProm
    .catch(async (err) => {
      const errMsg = err instanceof Error ? err.toString() :
                                            "unknown error";
      const warning = `Unable to call \`instantiateStreaming\` on WASM: ${errMsg}`;
      triggerMessage([ OutgoingMessageType.InitializationWarning,
                       warning ]);

      const res = await fetchedWasm;
      if (res.status < 200 || res.status >= 300) {
        const reqErr = new Error("WebAssembly request failed. status: " +
                                 String(res.status));
        throw reqErr;
      }

      const resAb = await res.arrayBuffer();
      return WebAssembly.instantiate(resAb, imports);
    })
    .then((instanceWasm) => {
      instance = instanceWasm;

        // TODO better types?
      linearMemory = instance.instance.exports.memory as WebAssembly.Memory;
    });
}

let isParsing = false;
function parseMpd(
  mpd : ArrayBuffer
) : void {
  readTimer = 0;
  timer = 0;
  if (instance === null) {
    throw new Error("DashWasmParser not initialized");
  }
  if (isParsing) {
    throw new Error("Parsing operation already pending.");
  }
  isParsing = true;
  mpdData = { mpd, cursor: 0 };

  try {
    // TODO better type this
    (instance.instance.exports.parse as () => void)();
  } catch (err) {
    isParsing = false;
    throw err;
  }
  isParsing = false;
  console.error("!!!!!!!! Parse TIMER", timer, readTimer);
}

function parseXlink(
  xlinkData : ArrayBuffer
) : void {
  if (instance === null) {
    throw new Error("DashWasmParser not initialized");
  }
  if (isParsing) {
    throw new Error("Parsing operation already pending.");
  }
  isParsing = true;
  mpdData = { mpd: xlinkData, cursor: 0 };

  try {
    // TODO better type this
    (instance.instance.exports.parse_xlink as () => void)();
  } catch (err) {
    isParsing = false;
    throw err;
  }
  isParsing = false;
}

/**
 * Callback called when a new Element has been encountered by the WASM parser.
 * @param {number} tag - Identify the tag encountered (@see TagName)
 */
function onTagOpen(tag : TagName) : void {
  const p = performance.now();
  triggerMessage([ OutgoingMessageType.TagOpen, tag ]);
  timer += (performance.now() - p);
}

/**
 * Callback called when an open Element's ending tag has been encountered by
 * the WASM parser.
 * @param {number} tag - Identify the tag in question (@see TagName)
 */
function onTagClose(tag : TagName) : void {
  const p = performance.now();
  triggerMessage([ OutgoingMessageType.TagClose, tag ]);
  timer += (performance.now() - p);
}

/**
 * Callback called each time a new Element's attribute is encountered by
 * the WASM parser.
 *
 * TODO Merge all attributes into the same callback with `onTagOpen`? I
 * tried but there's some difficulties if doing that.
 *
 * @param {number} attr - Identify the Attribute in question (@see TagName)
 * @param {number} ptr - Pointer to the first byte containing the
 * attribute's data in the WebAssembly's linear memory.
 * @param {number} len - Length of the attribute's value, in bytes.
 */
function onAttribute(attr : AttributeName, ptr : number, len : number) : void {
  const p = performance.now();
  let payload : ArrayBuffer;
  if (attr !== AttributeName.EventStreamEltRange) {
    payload = (linearMemory as WebAssembly.Memory)
      .buffer.slice(ptr, ptr + len);
  } else {
    // XXX TODO comment
    if (mpdData === null) {
      return;
    }
    const dataView = new DataView((linearMemory as WebAssembly.Memory).buffer);
    const rangeStart = dataView.getFloat64(ptr, true);
    const rangeEnd = dataView.getFloat64(ptr + 8, true);
    payload = mpdData.mpd.slice(rangeStart, rangeEnd);
  }
  triggerMessage([ OutgoingMessageType.Attribute, payload, attr ]);
  timer += (performance.now() - p);
}

/**
 * Callback called on the various "custom events" triggered by the WASM.
 *
 * @see CustomEventType
 * @param {number} evt - The type of the event
 * @param {number} ptr - Pointer to the first byte of the event's payload in
 * the WebAssembly's linear memory.
 * @param {number} len - Length of the payload, in bytes.
 */
function onCustomEvent(evt : CustomEventType, ptr : number, len : number) : void {
  const p = performance.now();
  const payload = (linearMemory as WebAssembly.Memory)
    .buffer.slice(ptr, ptr + len);

  if (__DEV__) {
    if (evt === CustomEventType.Log) {
      // "Log" are for debugging purpose-only.
      // It's safe to just console.log it out here.
      /* eslint-disable no-console */
      console.warn(new TextDecoder().decode(payload));
      /* eslint-enable no-console */
    }
  }
  if (evt === CustomEventType.Error) {
    triggerMessage([ OutgoingMessageType.ParserWarning,
                     payload ]);
  }
  timer += (performance.now() - p);
}

/**
 * Callback called by the WebAssembly when it needs to read new data from
 * the MPD.
 *
 * @param {number} ptr - First byte offset, in the WebAssembly's linear
 * memory, where the MPD should be set (under an array of bytes form).
 * @param {number} wantedSize - Size of the data, in bytes, asked by the
 * WebAssembly parser. It might receive less depending on if there's less
 * data in the MPD or if it goes over the set maximum size it could read
 * at a time.
 * @returns {number} - Return the number of bytes effectively read and set
 * in WebAssembly's linear memory (at the `ptr` offset).
 */
function readNext(ptr : number, wantedSize : number) : number {
  const p = performance.now();
  if (mpdData === null)  {
    throw new Error("DashWasmParser Error: No MPD to read.");
  }
  const { mpd, cursor } = mpdData;
  const sizeToRead = Math.min(wantedSize, MAX_READ_SIZE, mpd.byteLength - cursor);
  const arr = new Uint8Array((linearMemory as WebAssembly.Memory).buffer,
                             ptr, sizeToRead);
  arr.set(new Uint8Array(mpd, cursor, sizeToRead));
  mpdData.cursor += sizeToRead;
  readTimer += (performance.now() - p);
  return sizeToRead;
}
