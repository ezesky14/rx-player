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
  interval as observableInterval,
  map,
  merge as observableMerge,
  Observable,
  of as observableOf,
  startWith,
  Subscription,
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
  Adaptation, IRepresentationIndex,
} from "../../manifest";
import { fromEvent } from "../../utils/event_emitter";
import createSharedReference, {
  ISharedReference,
} from "../../utils/reference";

/** Number of seconds in a regular year. */
const YEAR_IN_SECONDS = 365 * 24 * 3600;

/**
 * Keep the MediaSource's duration up-to-date with what is being played.
 * @class MediaDurationUpdater
 */
export default class MediaDurationUpdater {
  private _subscription : Subscription;
  /**
   * The last known audio Adaptation (i.e. track) chosen for the last Period.
   * Useful to determinate the duration of the current content.
   * `undefined` if the audio track for the last Period has never been known yet.
   * `null` if there are no chosen audio Adaptation.
   */
  private _lastAudioRepresentation : ISharedReference<undefined |
                                                      Adaptation |
                                                      null>;
  /**
   * The last known video Adaptation (i.e. track) chosen for the last Period.
   * Useful to determinate the duration of the current content.
   * `undefined` if the video track for the last Period has never been known yet.
   * `null` if there are no chosen video Adaptation.
   */
  private _lastVideoRepresentation : ISharedReference<undefined |
                                                      Adaptation |
                                                      null>;

  /**
   * Create a new `MediaDurationUpdater` that will keep the given MediaSource's
   * duration as soon as possible.
   * This duration will be updated until the `stop` method is called.
   * @param {Object} manifest - The Manifest currently played.
   * For another content, you will have to create another `MediaDurationUpdater`.
   * @param {MediaSource} mediaSource - The MediaSource on which the content is
   * pushed.
   * @param {HTMLMediaElement} mediaElement
   */
  constructor(
    manifest : Manifest,
    mediaSource : MediaSource,
    mediaElement : HTMLMediaElement
  ) {
    this._lastAudioRepresentation = createSharedReference(undefined);
    this._lastVideoRepresentation = createSharedReference(undefined);
    this._subscription = isMediaSourceOpened$(mediaSource).pipe(
      switchMap((canUpdate) =>
        canUpdate ? observableCombineLatest([this._lastAudioRepresentation.asObservable(),
                                             this._lastVideoRepresentation.asObservable(),
                                             fromEvent(manifest, "manifestUpdate")
                                               .pipe(startWith(null))]) :
                    EMPTY
      ),
      switchMap(([lastAudioAdapVal, lastVideoAdapVal]) =>
        whenSourceBuffersEndedUpdates$(mediaSource.sourceBuffers).pipe(
          take(1)
        ).pipe(tap(() => {
          setMediaSourceDuration(mediaSource,
                                 mediaElement,
                                 manifest,
                                 lastAudioAdapVal,
                                 lastVideoAdapVal);
        }))
      )).subscribe();
  }

  /**
   * By default, the `MediaDurationUpdater` only set a safe estimate for the
   * MediaSource's duration.
   * A more precize duration can be set by communicating the video Adaptation
   * last loaded for the last Period.
   * @param {Object} adaptation
   */
  public updateLastVideoAdaptation(adaptation : Adaptation | null) {
    this._lastVideoRepresentation.setValue(adaptation);
  }

  /**
   * By default, the `MediaDurationUpdater` only set a safe estimate for the
   * MediaSource's duration.
   * A more precize duration can be set by communicating the audio Adaptation
   * last loaded for the last Period.
   * @param {Object} adaptation
   */
  public updateLastAudioAdaptation(adaptation : Adaptation | null) {
    this._lastAudioRepresentation.setValue(adaptation);
  }

  /**
   * Stop the `MediaDurationUpdater` from updating and free its resources.
   * Once stopped, it is not possible to start it again, beside creating another
   * `MediaDurationUpdater`.
   */
  public stop() {
    this._subscription.unsubscribe();
  }
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
 * @param {HTMLMediaElement} mediaElement
 * @param {Object} manifest
 * @returns {Observable.<number | null>}
 */
function setMediaSourceDuration(
  mediaSource: MediaSource,
  mediaElement: HTMLMediaElement,
  manifest: Manifest,
  lastAudioAdaptationRef : undefined | Adaptation | null,
  lastVideoAdaptationRef : undefined | Adaptation | null
): number | null {
  const newDuration = getCalculatedContentDuration(manifest,
                                                   lastAudioAdaptationRef,
                                                   lastVideoAdaptationRef);
  const bufferedLen = mediaElement.buffered.length;
  if ((bufferedLen > 0 && mediaElement.buffered.end(bufferedLen - 1) >= newDuration)) {
    // We already buffered further than the duration we want to set.
    // Keep the duration that was set at that time as a security.
    return null;
  }
  if (isNaN(mediaSource.duration) || !isFinite(mediaSource.duration) ||
      newDuration - mediaSource.duration > 0.01) {
    try {
      log.info("Init: Updating duration", newDuration);
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

  /**
   * Some Manifest parsers use the exact same `IRepresentationIndex` reference
   * for each Representation of a given Adaptation, because in the actual source
   * Manifest file, indexing data is often defined at Adaptation-level.
   * This variable allows to optimize the logic here when this is the case.
   */
  let lastIndex : IRepresentationIndex | undefined;
  for (let i = 0; i < representations.length; i++) {
    if (representations[i].index !== lastIndex) {
      lastIndex = representations[i].index;
      const lastPosition = representations[i].index.getLastPosition();
      if (lastPosition === undefined) { // we cannot tell
        return undefined;
      }
      if (lastPosition !== null) {
        min = min == null ? lastPosition :
                            Math.min(min, lastPosition);
      }
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
