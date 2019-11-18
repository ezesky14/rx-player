/**
 * Copyright 2019 CANAL+ Group
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

import { AsyncSubject, combineLatest, of } from "rxjs";
import { filter, startWith } from "rxjs/operators";

import { SegmentPipelinesManager } from "../../../../../core/pipelines";
import { IInitSettings, IStoredManifest } from "../../types";
import { initDownloader$ } from "./initSegment";
import { getTransportPipelineByTransport } from "./manifest";
import { segmentPipelineDownloader$ } from "./segment";
import { IUtilsNotification } from "./types";

class DownloadManager {
  readonly utils: IUtilsNotification;

  constructor(utils: IUtilsNotification) {
    this.utils = utils;
  }

  initDownload(initSettings: IInitSettings, pause$: AsyncSubject<void>) {
    const { contentID } = initSettings;
    const builderInit = {
      progress: { percentage: 0, current: 0, overall: 0 },
      manifest: null,
      video: [],
      audio: [],
      text: [],
      size: 0,
    };
    const pipelineSegmentDownloader$ = segmentPipelineDownloader$(
      initDownloader$(initSettings, this.utils.db),
      builderInit,
      { contentID, db: this.utils.db, pause$, emitter: this.utils.emitter }
    );
    return combineLatest([
      pipelineSegmentDownloader$.pipe(
        // TODO: See what we can do here with, this define the frequency of save
        filter(({ progress: { percentage } }) => percentage % 10 === 0),
        startWith(null)
      ),
      pause$.pipe(startWith(null)),
    ]);
  }

  resumeDownload(resumeSettings: IStoredManifest, pause$: AsyncSubject<void>) {
    const {
      progress,
      manifest,
      builder: { video, audio, text },
      contentID,
      transport,
      size,
    } = resumeSettings;
    const segmentPipelinesManager = new SegmentPipelinesManager<any>(
      getTransportPipelineByTransport(transport),
      {
        lowLatencyMode: false,
      }
    );
    const builderInit = {
      progress,
      manifest,
      video,
      audio,
      text,
      size,
    };
    const pipelineSegmentDownloader$ = segmentPipelineDownloader$(
      of({
        progress,
        video,
        audio,
        text,
        manifest,
        segmentPipelinesManager,
        type: "resume",
      }),
      builderInit,
      { contentID, db: this.utils.db, pause$, emitter: this.utils.emitter }
    );
    return combineLatest([
      pipelineSegmentDownloader$.pipe(
        // TODO: See what we can do here with, this define the frequency of save
        filter(({ progress: { percentage } }) => percentage % 10 === 0),
        startWith(null)
      ),
      pause$.pipe(startWith(null)),
    ]);
  }
}

export default DownloadManager;