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
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    function adopt(value) { return value instanceof P ? value : new P(function (resolve) { resolve(value); }); }
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
var __generator = (this && this.__generator) || function (thisArg, body) {
    var _ = { label: 0, sent: function() { if (t[0] & 1) throw t[1]; return t[1]; }, trys: [], ops: [] }, f, y, t, g;
    return g = { next: verb(0), "throw": verb(1), "return": verb(2) }, typeof Symbol === "function" && (g[Symbol.iterator] = function() { return this; }), g;
    function verb(n) { return function (v) { return step([n, v]); }; }
    function step(op) {
        if (f) throw new TypeError("Generator is already executing.");
        while (_) try {
            if (f = 1, y && (t = op[0] & 2 ? y["return"] : op[0] ? y["throw"] || ((t = y["return"]) && t.call(y), 0) : y.next) && !(t = t.call(y, op[1])).done) return t;
            if (y = 0, t) op = [op[0] & 2, t.value];
            switch (op[0]) {
                case 0: case 1: t = op; break;
                case 4: _.label++; return { value: op[1], done: false };
                case 5: _.label++; y = op[1]; op = [0]; continue;
                case 7: op = _.ops.pop(); _.trys.pop(); continue;
                default:
                    if (!(t = _.trys, t = t.length > 0 && t[t.length - 1]) && (op[0] === 6 || op[0] === 2)) { _ = 0; continue; }
                    if (op[0] === 3 && (!t || (op[1] > t[0] && op[1] < t[3]))) { _.label = op[1]; break; }
                    if (op[0] === 6 && _.label < t[1]) { _.label = t[1]; t = op; break; }
                    if (t && _.label < t[2]) { _.label = t[2]; _.ops.push(op); break; }
                    if (t[2]) _.ops.pop();
                    _.trys.pop(); continue;
            }
            op = body.call(thisArg, _);
        } catch (e) { op = [6, e]; y = 0; } finally { f = t = 0; }
        if (op[0] & 5) throw op[1]; return { value: op[0] ? op[1] : void 0, done: true };
    }
};
import PPromise from "pinkie";
import log from "../../../../../log";
import assertUnreachable from "../../../../../utils/assert_unreachable";
import noop from "../../../../../utils/noop";
import parseMpdIr from "../../common";
import { generateRootChildrenParser } from "./generators";
import { generateXLinkChildrenParser } from "./generators/XLink";
import ParsersStack from "./parsers_stack";
var MAX_READ_SIZE = 15e3;
var DashWasmParser = /** @class */ (function () {
    /**
     * Create a new `DashWasmParser`.
     * @param {object} opts
     */
    function DashWasmParser() {
        this._parsersStack = new ParsersStack();
        this._instance = null;
        this._mpdData = null;
        this._linearMemory = null;
        this.status = "uninitialized";
        this._initProm = null;
        this._warnings = [];
        this._isParsing = false;
    }
    /**
     * Returns Promise that will resolve when the initialization has ended (either
     * with success, in which cases the Promise resolves, either with failure, in
     * which case it rejects the corresponding error).
     *
     * This is actually the exact same Promise than the one returned by the first
     * `initialize` call.
     *
     * If that method was never called, returns a rejecting Promise.
     * @returns {Promise}
     */
    DashWasmParser.prototype.waitForInitialization = function () {
        var _a;
        return (_a = this._initProm) !== null && _a !== void 0 ? _a : PPromise.reject("No initialization performed yet.");
    };
    DashWasmParser.prototype.initialize = function (opts) {
        return __awaiter(this, void 0, void 0, function () {
            /**
             * Callback called when a new Element has been encountered by the WASM parser.
             * @param {number} tag - Identify the tag encountered (@see TagName)
             */
            function onTagOpen(tag) {
                // Call the active "childrenParser"
                return parsersStack.childrenParser(tag);
            }
            /**
             * Callback called when an open Element's ending tag has been encountered by
             * the WASM parser.
             * @param {number} tag - Identify the tag in question (@see TagName)
             */
            function onTagClose(tag) {
                // Only pop current parsers from the `parsersStack` if that tag was the
                // active one.
                return parsersStack.popIfCurrent(tag);
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
            function onAttribute(attr, ptr, len) {
                // Call the active "attributeParser"
                return parsersStack.attributeParser(attr, ptr, len);
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
            function onCustomEvent(evt, ptr, len) {
                var linearMemory = self._linearMemory;
                var arr = new Uint8Array(linearMemory.buffer, ptr, len);
                if (evt === 1 /* Error */) {
                    var decoded = textDecoder.decode(arr);
                    log.warn("WASM Error Event:", decoded);
                    self._warnings.push(new Error(decoded));
                }
                else if (evt === 0 /* Log */) {
                    var decoded = textDecoder.decode(arr);
                    log.warn("WASM Log Event:", decoded);
                }
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
            function readNext(ptr, wantedSize) {
                if (self._mpdData === null) {
                    throw new Error("DashWasmParser Error: No MPD to read.");
                }
                var linearMemory = self._linearMemory;
                var _a = self._mpdData, mpd = _a.mpd, cursor = _a.cursor;
                var sizeToRead = Math.min(wantedSize, MAX_READ_SIZE, mpd.byteLength - cursor);
                var arr = new Uint8Array(linearMemory.buffer, ptr, sizeToRead);
                arr.set(new Uint8Array(mpd, cursor, sizeToRead));
                self._mpdData.cursor += sizeToRead;
                return sizeToRead;
            }
            var parsersStack, textDecoder, self, imports, fetchedWasm, streamingProm;
            var _this = this;
            return __generator(this, function (_a) {
                if (this.status !== "uninitialized") {
                    return [2 /*return*/, PPromise.reject(new Error("DashWasmParser already initialized."))];
                }
                else if (!this.isCompatible()) {
                    this.status = "failure";
                    return [2 /*return*/, PPromise.reject(new Error("Target not compatible with WebAssembly."))];
                }
                this.status = "initializing";
                parsersStack = this._parsersStack;
                textDecoder = new TextDecoder();
                self = this;
                imports = {
                    env: {
                        memoryBase: 0,
                        tableBase: 0,
                        memory: new WebAssembly.Memory({ initial: 10 }),
                        table: new WebAssembly.Table({ initial: 2, element: "anyfunc" }),
                        onTagOpen: onTagOpen,
                        onCustomEvent: onCustomEvent,
                        onAttribute: onAttribute,
                        readNext: readNext,
                        onTagClose: onTagClose,
                    },
                };
                fetchedWasm = fetch(opts.wasmUrl);
                streamingProm = typeof WebAssembly.instantiateStreaming === "function" ?
                    WebAssembly.instantiateStreaming(fetchedWasm, imports) :
                    PPromise.reject("`WebAssembly.instantiateStreaming` API not available");
                this._initProm = streamingProm
                    .catch(function (e) { return __awaiter(_this, void 0, void 0, function () {
                    var res, resAb;
                    return __generator(this, function (_a) {
                        switch (_a.label) {
                            case 0:
                                log.warn("Unable to call `instantiateStreaming` on WASM:", e);
                                return [4 /*yield*/, fetchedWasm];
                            case 1:
                                res = _a.sent();
                                if (res.status < 200 || res.status >= 300) {
                                    throw new Error("WebAssembly request failed. status: " + String(res.status));
                                }
                                return [4 /*yield*/, res.arrayBuffer()];
                            case 2:
                                resAb = _a.sent();
                                return [2 /*return*/, WebAssembly.instantiate(resAb, imports)];
                        }
                    });
                }); })
                    .then(function (instanceWasm) {
                    _this._instance = instanceWasm;
                    // TODO better types?
                    _this._linearMemory = _this._instance.instance.exports.memory;
                    _this.status = "initialized";
                }).catch(function (err) {
                    var message = err instanceof Error ? err.toString() :
                        "Unknown error";
                    log.warn("DW: Could not create DASH-WASM parser:", message);
                    _this.status = "failure";
                });
                return [2 /*return*/, this._initProm];
            });
        });
    };
    /**
     * @param {Document} manifest - Original manifest as returned by the server
     * @param {Object} args
     * @returns {Object}
     */
    DashWasmParser.prototype.runWasmParser = function (mpd, args) {
        var _a = this._parseMpd(mpd), mpdIR = _a[0], warnings = _a[1];
        if (mpdIR === null) {
            throw new Error("DASH Parser: Unknown error while parsing the MPD");
        }
        var ret = parseMpdIr(mpdIR, args, warnings);
        return this._processParserReturnValue(ret);
    };
    /**
     * Return `true` if the current plaform is compatible with WebAssembly and the
     * TextDecoder interface (for faster UTF-8 parsing), which are needed features
     * for the `DashWasmParser`.
     * @returns {boolean}
     */
    DashWasmParser.prototype.isCompatible = function () {
        return typeof WebAssembly === "object" &&
            typeof WebAssembly.instantiate === "function" &&
            typeof window.TextDecoder === "function";
    };
    DashWasmParser.prototype._parseMpd = function (mpd) {
        var _a;
        if (this._instance === null) {
            throw new Error("DashWasmParser not initialized");
        }
        if (this._isParsing) {
            throw new Error("Parsing operation already pending.");
        }
        this._isParsing = true;
        this._mpdData = { mpd: mpd, cursor: 0 };
        var rootObj = {};
        var linearMemory = this._linearMemory;
        var rootChildrenParser = generateRootChildrenParser(rootObj, linearMemory, this._parsersStack, mpd);
        this._parsersStack.pushParsers(null, rootChildrenParser, noop);
        this._warnings = [];
        try {
            // TODO better type this
            this._instance.instance.exports.parse();
        }
        catch (err) {
            this._parsersStack.reset();
            this._warnings = [];
            this._isParsing = false;
            throw err;
        }
        var parsed = (_a = rootObj.mpd) !== null && _a !== void 0 ? _a : null;
        var warnings = this._warnings;
        this._parsersStack.reset();
        this._warnings = [];
        this._isParsing = false;
        return [parsed, warnings];
    };
    DashWasmParser.prototype._parseXlink = function (xlinkData) {
        if (this._instance === null) {
            throw new Error("DashWasmParser not initialized");
        }
        if (this._isParsing) {
            throw new Error("Parsing operation already pending.");
        }
        this._isParsing = true;
        this._mpdData = { mpd: xlinkData, cursor: 0 };
        var rootObj = { periods: [] };
        var linearMemory = this._linearMemory;
        var xlinkParser = generateXLinkChildrenParser(rootObj, linearMemory, this._parsersStack, xlinkData);
        this._parsersStack.pushParsers(null, xlinkParser, noop);
        this._warnings = [];
        try {
            // TODO better type this
            this._instance.instance.exports.parse_xlink();
        }
        catch (err) {
            this._parsersStack.reset();
            this._warnings = [];
            this._isParsing = false;
            throw err;
        }
        var periods = rootObj.periods;
        var warnings = this._warnings;
        this._parsersStack.reset();
        this._warnings = [];
        this._isParsing = false;
        return [periods, warnings];
    };
    /**
     * Handle `parseMpdIr` return values, asking for resources if they are needed
     * and pre-processing them before continuing parsing.
     *
     * @param {Object} initialRes
     * @returns {Object}
     */
    DashWasmParser.prototype._processParserReturnValue = function (initialRes) {
        var _this = this;
        if (initialRes.type === "done") {
            return initialRes;
        }
        else if (initialRes.type === "needs-clock") {
            var continueParsingMPD = function (loadedClock) {
                if (loadedClock.length !== 1) {
                    throw new Error("DASH parser: wrong number of loaded ressources.");
                }
                var newRet = initialRes.value.continue(loadedClock[0].responseData);
                return _this._processParserReturnValue(newRet);
            };
            return { type: "needs-resources",
                value: { urls: [initialRes.value.url],
                    format: "string",
                    continue: continueParsingMPD } };
        }
        else if (initialRes.type === "needs-xlinks") {
            var continueParsingMPD = function (loadedXlinks) {
                var resourceInfos = [];
                for (var i = 0; i < loadedXlinks.length; i++) {
                    var _a = loadedXlinks[i], xlinkData = _a.responseData, receivedTime = _a.receivedTime, sendingTime = _a.sendingTime, url = _a.url;
                    var _b = _this._parseXlink(xlinkData), periodsIr = _b[0], periodsIRWarnings = _b[1];
                    resourceInfos.push({ url: url, receivedTime: receivedTime, sendingTime: sendingTime, parsed: periodsIr,
                        warnings: periodsIRWarnings });
                }
                var newRet = initialRes.value.continue(resourceInfos);
                return _this._processParserReturnValue(newRet);
            };
            return { type: "needs-resources",
                value: { urls: initialRes.value.xlinksUrls,
                    format: "arraybuffer",
                    continue: continueParsingMPD } };
        }
        else {
            assertUnreachable(initialRes);
        }
    };
    return DashWasmParser;
}());
export default DashWasmParser;
