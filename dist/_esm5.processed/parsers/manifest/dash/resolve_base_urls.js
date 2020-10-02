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
import arrayIncludes from "../../../utils/array_includes";
import resolveURL from "../../../utils/resolve_url";
/**
 * @param {Array.<string>} currentBaseURLs
 * @param {Array.<Object>} newBaseURLs
 * @returns {Array.<string>}
 */
export default function resolveBaseURLs(currentBaseURLs, newBaseURLs) {
    var result = [];
    if (newBaseURLs.length === 0) {
        return currentBaseURLs;
    }
    else if (currentBaseURLs.length === 0) {
        for (var i = 0; i < newBaseURLs.length; i++) {
            if (!arrayIncludes(result, newBaseURLs[i].value)) {
                result.push(newBaseURLs[i].value);
            }
        }
        return result;
    }
    else {
        for (var i = 0; i < currentBaseURLs.length; i++) {
            var rootURL = currentBaseURLs[i];
            for (var j = 0; j < newBaseURLs.length; j++) {
                var newURL = resolveURL(rootURL, newBaseURLs[j].value);
                if (!arrayIncludes(result, newURL)) {
                    result.push(newURL);
                }
            }
        }
    }
    return result;
}
