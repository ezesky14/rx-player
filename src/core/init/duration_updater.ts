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
  combineLatest as observableCombineLatest,
  distinctUntilChanged,
  EMPTY,
  filter,
  fromEvent as observableFromEvent,
  ignoreElements,
  interval as observableInterval,
  map,
  merge as observableMerge,
  Observable,
  of as observableOf,
  startWith,
  switchMap,
  take,
  tap,
} from "rxjs";
import {
  onSourceOpen$,
  onSourceClose$,
  onSourceEnded$,
} from "../../compat/event_listeners";
import log from "../../log";
import Manifest, {
  Adaptation,
} from "../../manifest";
import { fromEvent } from "../../utils/event_emitter";
import createSharedReference, {
  IReadOnlySharedReference,
} from "../../utils/reference";

/** Number of seconds in a regular year. */
const YEAR_IN_SECONDS = 365 * 24 * 3600;

/**
 * Keep the MediaSource duration up-to-date with the Manifest one on
 * subscription:
 * Set the current duration initially and then update if needed after
 * each Manifest updates.
 * @param {Object} manifest
 * @param {MediaSource} mediaSource
 * XXX TODO JSDoc
 * @returns {Observable}
 */
export default function DurationUpdater(
  manifest : Manifest,
  mediaSource : MediaSource,
  lastAudioAdaptationRef : IReadOnlySharedReference<undefined | Adaptation | null>,
  lastVideoAdaptationRef : IReadOnlySharedReference<undefined | Adaptation | null>
) : Observable<never> {
  const lastSetDuration = createSharedReference<number | undefined>(undefined);
  return isMediaSourceOpened$(mediaSource).pipe(
    switchMap((canUpdate) =>
      canUpdate ? observableCombineLatest([lastAudioAdaptationRef.asObservable(),
                                           lastVideoAdaptationRef.asObservable(),
                                           fromEvent(manifest, "manifestUpdate")
                                             .pipe(startWith(null))]) :
                  EMPTY
    ),
    switchMap(([lastAudioAdapVal, lastVideoAdapVal]) =>
      whenSourceBuffersEndedUpdates$(mediaSource.sourceBuffers).pipe(
        take(1)
      ).pipe(tap(() => {
        const newDuration = setMediaSourceDuration(mediaSource,
                                                   manifest,
                                                   lastSetDuration.getValue(),
                                                   lastAudioAdapVal,
                                                   lastVideoAdapVal);
        lastSetDuration.setValue(newDuration ?? undefined);
      }))
    ),
    // NOTE As of now (RxJS 7.4.0), RxJS defines `ignoreElements` default
    // first type parameter as `any` instead of the perfectly fine `unknown`,
    // leading to linter issues, as it forbids the usage of `any`.
    // This is why we're disabling the eslint rule.
    /* eslint-disable-next-line @typescript-eslint/no-unsafe-argument */
    ignoreElements());
}

/**
 * Checks that duration can be updated on the MediaSource, and then
 * sets it.
 *
 * Returns either:
 *   - the new duration it has been updated to if it has
 *   - `null` if it hasn'nt been updated
 *
 * @param {MediaSource} mediaSource
 * @param {Object} manifest
 * @param {number | undefined} lastSetDuration
 * @returns {Observable.<number | null>}
 */
function setMediaSourceDuration(
  mediaSource: MediaSource,
  manifest: Manifest,
  lastSetDuration: number | undefined,
  lastAudioAdaptationRef : undefined | Adaptation | null,
  lastVideoAdaptationRef : undefined | Adaptation | null
): number | null {
  const newDuration = getCalculatedContentDuration(manifest,
                                                   lastAudioAdaptationRef,
                                                   lastVideoAdaptationRef);
  // XXX TODO
  if (mediaSource.duration >= newDuration ||
      // Even if the MediaSource duration is different than the duration that
      // we want to set now, the last duration we wanted to set may be the same,
      // as the MediaSource duration may have been changed by the browser.
      //
      // In that case, we do not want to update it.
      //
      newDuration === lastSetDuration) {
    return null;
  }
  if (isNaN(mediaSource.duration) || !isFinite(mediaSource.duration) ||
      newDuration - mediaSource.duration > 0.01) {
    log.info("Init: Updating duration", newDuration);
    try {

      mediaSource.duration = newDuration;
    } catch (err) {
      log.warn("Duration Updater: Can't update duration on the MediaSource.", err);
      return null;
    }
    return newDuration;
  }
  return null;
}

/**
 * Returns an Observable which will emit only when all the SourceBuffers ended
 * all pending updates.
 * @param {SourceBufferList} sourceBuffers
 * @returns {Observable}
 */
function whenSourceBuffersEndedUpdates$(
  sourceBuffers: SourceBufferList
) : Observable<undefined> {
  if (sourceBuffers.length === 0) {
    return observableOf(undefined);
  }
  const sourceBufferUpdatingStatuses : Array<Observable<boolean>> = [];

  for (let i = 0; i < sourceBuffers.length; i++) {
    const sourceBuffer = sourceBuffers[i];
    sourceBufferUpdatingStatuses.push(
      observableMerge(
        observableFromEvent(sourceBuffer, "updatestart").pipe(map(() => true)),
        observableFromEvent(sourceBuffer, "update").pipe(map(() => false)),
        observableInterval(500).pipe(map(() => sourceBuffer.updating))
      ).pipe(
        startWith(sourceBuffer.updating),
        distinctUntilChanged()
      )
    );
  }
  return observableCombineLatest(sourceBufferUpdatingStatuses).pipe(
    filter((areUpdating) => {
      return areUpdating.every((isUpdating) => !isUpdating);
    }),
    map(() => undefined)
  );
}

function getCalculatedContentDuration(
  manifest : Manifest,
  lastAudioAdaptationRef : undefined | Adaptation | null,
  lastVideoAdaptationRef : undefined | Adaptation | null
) : number {
  if (manifest.isDynamic) {
    const maximumPosition = manifest.getMaximumPosition();
    // Some targets poorly support setting a very high number for durations.
    // Yet, in dynamic contents, we would prefer setting a value as high as possible
    // to still be able to seek anywhere we want to (even ahead of the Manifest if
    // we want to). As such, we put it at a safe default value of 2^32 excepted
    // when the maximum position is already relatively close to that value, where
    // we authorize exceptionally going over it.
    return Math.max(Math.pow(2, 32), maximumPosition + YEAR_IN_SECONDS);
  } else {
    if (lastAudioAdaptationRef === undefined ||
        lastVideoAdaptationRef === undefined)
    {
      return manifest.getMaximumPosition();
    } else if (lastAudioAdaptationRef === null) {
      if (lastVideoAdaptationRef === null) {
        return manifest.getMaximumPosition();
      } else {
        const lastVideoPosition =
          getLastPositionFromAdaptation(lastVideoAdaptationRef);
        if (typeof lastVideoPosition !== "number") {
          return manifest.getMaximumPosition();
        }
        return lastVideoPosition;
      }
    } else if (lastVideoAdaptationRef === null) {
      const lastAudioPosition =
        getLastPositionFromAdaptation(lastAudioAdaptationRef);
      if (typeof lastAudioPosition !== "number") {
        return manifest.getMaximumPosition();
      }
      return lastAudioPosition;
    } else {
      const lastAudioPosition = getLastPositionFromAdaptation(
        lastAudioAdaptationRef
      );
      const lastVideoPosition = getLastPositionFromAdaptation(
        lastVideoAdaptationRef
      );
      if (typeof lastAudioPosition !== "number" ||
          typeof lastVideoPosition !== "number")
      {
        return manifest.getMaximumPosition();
      } else {
        return Math.min(lastAudioPosition, lastVideoPosition);
      }
    }
  }
}

/**
 * Returns "last time of reference" from the adaptation given.
 * `undefined` if a time could not be found.
 * Null if the Adaptation has no segments (it could be that it didn't started or
 * that it already finished for example).
 *
 * We consider the earliest last time from every representations in the given
 * adaptation.
 * @param {Object} adaptation
 * @returns {Number|undefined|null}
 */
function getLastPositionFromAdaptation(
  adaptation: Adaptation
) : number | undefined | null {
  const { representations } = adaptation;
  let min : null | number = null;
  // XXX TODO optimize when index is the same?
  for (let i = 0; i < representations.length; i++) {
    const lastPosition = representations[i].index.getLastPosition();
    if (lastPosition === undefined) { // we cannot tell
      return undefined;
    }
    if (lastPosition !== null) {
      min = min == null ? lastPosition :
                          Math.min(min, lastPosition);
    }
  }
  if (min === null) { // It means that all positions were null === no segments (yet?)
    return null;
  }
  return min;
}

/**
 * Emit a boolean that tells if the media source is opened or not.
 * @param {MediaSource} mediaSource
 * @returns {Object}
 */
function isMediaSourceOpened$(mediaSource: MediaSource): Observable<boolean> {
  return observableMerge(onSourceOpen$(mediaSource).pipe(map(() => true)),
                         onSourceEnded$(mediaSource).pipe(map(() => false)),
                         onSourceClose$(mediaSource).pipe(map(() => false))
  ).pipe(
    startWith(mediaSource.readyState === "open"),
    distinctUntilChanged()
  );
}
/* eslint-enable @typescript-eslint/no-unsafe-argument */
