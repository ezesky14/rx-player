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
 * This file allows any Stream to push data to a SegmentBuffer.
 */

import { MediaError } from "../../../errors";
import { CancellationSignal } from "../../../utils/task_canceller";
import { IReadOnlyPlaybackObserver } from "../../api";
import {
  IPushChunkInfos,
  SegmentBuffer,
} from "../../segment_buffers";
import forceGarbageCollection from "./force_garbage_collection";

/**
 * Append a segment to the given segmentBuffer.
 * If it leads to a QuotaExceededError, try to run our custom range
 * _garbage collector_ then retry.
 * @param {Observable} playbackObserver
 * @param {Object} segmentBuffer
 * @param {Object} dataInfos
 * @param {Object} cancellationSignal
 * @returns {Promise}
 */
export default async function appendSegmentToBuffer<T>(
  playbackObserver : IReadOnlyPlaybackObserver<ICurrentPositionInformation>,
  segmentBuffer : SegmentBuffer,
  dataInfos : IPushChunkInfos<T>,
  cancellationSignal : CancellationSignal
) : Promise<void> {
  try {
    await segmentBuffer.pushChunk(dataInfos, cancellationSignal);
  } catch (appendError : unknown) {
    if (!(appendError instanceof Error) || appendError.name !== "QuotaExceededError") {
      const reason = appendError instanceof Error ?
        appendError.toString() :
        "An unknown error happened when pushing content";
      throw new MediaError("BUFFER_APPEND_ERROR", reason);
    }

    // TODO so awkward right now, it should just be possible to obtain the last
    // observation synchronously
    const { position, wantedTimeOffset } = await new Promise<
      ICurrentPositionInformation
    >((res) => {
      const unlisten = playbackObserver.listen(res, { includeLastObservation: true });
      unlisten(); // We already received the last observation (synchronously)
    });

    const currentPos = position + wantedTimeOffset;
    try {
      await forceGarbageCollection(currentPos, segmentBuffer, cancellationSignal);
      await segmentBuffer.pushChunk(dataInfos, cancellationSignal);
    } catch (err2) {
      const reason = err2 instanceof Error ? err2.toString() :
                                             "Could not clean the buffer";

      throw new MediaError("BUFFER_FULL_ERROR", reason);
    }
  }
}

interface ICurrentPositionInformation {
  position : number;
  wantedTimeOffset : number;
}
