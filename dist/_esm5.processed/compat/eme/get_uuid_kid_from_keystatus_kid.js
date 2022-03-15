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
import { guidToUuid } from "../../utils/string_parsing";
import { isEdgeChromium, isIEOrEdge, } from "../browser_detection";
/**
 * Get KID from MediaKeySession keyStatus, and convert it in usual big-endian kid
 * if necessary. On EDGE, Microsoft Playready KID are presented into little-endian GUID.
 * @param {String} keySystem
 * @param {Uint8Array} baseKeyId
 * @returns {Uint8Array}
 */
export default function getUUIDKIDFromKeyStatusKID(keySystem, baseKeyId) {
    if (keySystem.indexOf("playready") !== -1 &&
        (isIEOrEdge || isEdgeChromium)) {
        return guidToUuid(baseKeyId);
    }
    return baseKeyId;
}
