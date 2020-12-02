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
import Manifest from "../../manifest";
import { IInitClockTick } from "./types";
/**
 * Perform various checks about discontinuities during playback.
 * If a discontinuity is encountered, return the theorical end of
 * discontinuity.
 * @param {Observable} clock$
 * @param {Object} manifest
 * @returns {Observable}
 */
export default function getCurrentDiscontinuityEnd(clock$: Observable<IInitClockTick>, manifest: Manifest): Observable<number>;