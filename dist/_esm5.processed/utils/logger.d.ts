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
export declare type ILoggerLevel = "NONE" | "ERROR" | "WARNING" | "INFO" | "DEBUG";
declare type IConsoleFn = (...args: unknown[]) => void;
/**
 * Logger implementation.
 * @class Logger
 */
export default class Logger {
    error: IConsoleFn;
    warn: IConsoleFn;
    info: IConsoleFn;
    debug: IConsoleFn;
    private _currentLevel;
    private readonly _levels;
    constructor();
    /**
     * @param {string} levelStr
     */
    setLevel(levelStr: string): void;
    /**
     * @returns {string}
     */
    getLevel(): ILoggerLevel;
}
export {};