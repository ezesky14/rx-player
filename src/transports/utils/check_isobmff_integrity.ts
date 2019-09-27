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

import { OtherError } from "../../errors";
import findBox from "./find_box";

/**
 * @param {Uint8Array} buffer
 * @param {boolean} isInit
 * @returns {Array}
 */
export default function checkISOBMFFIntegrity(
  buffer : Uint8Array,
  isInit : boolean
) : void {
  if (isInit) {
    const ftypIndex = findBox(buffer, 0x66747970 /* mdat */);
    if (ftypIndex < 0) {
      throw new OtherError("INTEGRITY_ERROR", "Incomplete `ftyp` box");
    }
    const moovIndex = findBox(buffer, 0x6d6f6f76 /* moof */);
    if (moovIndex < 0) {
      throw new OtherError("INTEGRITY_ERROR", "Incomplete `moov` box");
    }
  } else {
    const moofIndex = findBox(buffer, 0x6d6f6f66 /* moof */);
    if (moofIndex < 0) {
      throw new OtherError("INTEGRITY_ERROR", "Incomplete `moof` box");
    }
    const mdatIndex = findBox(buffer, 0x6d646174 /* mdat */);
    if (mdatIndex < 0) {
      throw new OtherError("INTEGRITY_ERROR", "Incomplete `mdat` box");
    }
  }
}
