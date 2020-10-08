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
/**
 * Request fullScreen action on a given element.
 * @param {HTMLElement} elt
 */
declare function requestFullscreen(element: HTMLMediaElement): void;
/**
 * Exit fullscreen if an element is currently in fullscreen.
 */
declare function exitFullscreen(): void;
/**
 * Returns true if an element in the document is being displayed in fullscreen
 * mode;
 * otherwise it's false.
 * @returns {boolean}
 */
declare function isFullscreen(): boolean;
export { requestFullscreen, exitFullscreen, isFullscreen, };
