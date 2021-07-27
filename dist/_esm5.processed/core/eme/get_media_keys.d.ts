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
import { Observable } from "rxjs";
import { ICustomMediaKeys, ICustomMediaKeySystemAccess } from "../../compat";
import { IKeySystemOption, IMediaKeySessionStores } from "./types";
/** Object returned by `getMediaKeysInfos`. */
export interface IMediaKeysInfos {
    /** The MediaKeySystemAccess which allowed to create the MediaKeys instance. */
    mediaKeySystemAccess: MediaKeySystemAccess | ICustomMediaKeySystemAccess;
    /** The MediaKeys instance. */
    mediaKeys: MediaKeys | ICustomMediaKeys;
    /** Stores allowing to create and retrieve MediaKeySessions. */
    stores: IMediaKeySessionStores;
    /** IKeySystemOption compatible to the created MediaKeys instance. */
    options: IKeySystemOption;
}
/**
 * @param {HTMLMediaElement} mediaElement
 * @param {Array.<Object>} keySystemsConfigs
 * @returns {Observable}
 */
export default function getMediaKeysInfos(mediaElement: HTMLMediaElement, keySystemsConfigs: IKeySystemOption[]): Observable<IMediaKeysInfos>;