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
  catchError,
  concat as observableConcat,
  EMPTY,
  filter,
  ignoreElements,
  map,
  mapTo,
  merge as observableMerge,
  mergeMap,
  Observable,
  of as observableOf,
  shareReplay,
  take,
  tap,
} from "rxjs";
import {
  events,
  generateKeyRequest,
  getInitData,
  ICustomMediaKeySystemAccess,
} from "../../compat/";
import config from "../../config";
import { EncryptedMediaError } from "../../errors";
import log from "../../log";
import areArraysOfNumbersEqual from "../../utils/are_arrays_of_numbers_equal";
import arrayFind from "../../utils/array_find";
import arrayIncludes from "../../utils/array_includes";
import assertUnreachable from "../../utils/assert_unreachable";
import { concat } from "../../utils/byte_parsing";
import filterMap from "../../utils/filter_map";
import isNullOrUndefined from "../../utils/is_null_or_undefined";
import createSharedReference from "../../utils/reference";
import cleanOldStoredPersistentInfo from "./clean_old_stored_persistent_info";
import createOrLoadSession from "./get_session";
import initMediaKeys from "./init_media_keys";
import SessionEventsListener, {
  BlacklistedSessionError,
} from "./session_events_listener";
import setServerCertificate from "./set_server_certificate";
import {
  IAttachedMediaKeysEvent,
  IContentProtection,
  ICreatedMediaKeysEvent,
  IEMEManagerEvent,
  IEMEWarningEvent,
  IInitializationDataInfo,
  IKeySystemOption,
} from "./types";
import ProcessedInitDataRecord, {
  areAllKeyIdContainedIn,
  areSomeKeyIdContainedIn,
} from "./utils/processed_init_data_record";

const { EME_DEFAULT_MAX_SIMULTANEOUS_MEDIA_KEY_SESSIONS,
        EME_MAX_STORED_PERSISTENT_SESSION_INFORMATION } = config;
const { onEncrypted$ } = events;

/**
 * EME abstraction used to communicate with the Content Decryption Module (or
 * CDM) to be able to decrypt contents.
 *
 * The `EMEManager` can be given one or multiple key systems. It will choose the
 * appropriate one depending on user settings and browser support.
 * @param {HTMLMediaElement} mediaElement - The MediaElement which will be
 * associated to a MediaKeys object
 * @param {Array.<Object>} keySystemsConfigs - key system configuration
 * @param {Observable} contentProtections$ - Observable emitting external
 * initialization data.
 * @returns {Observable}
 */
export default function EMEManager(
  mediaElement : HTMLMediaElement,
  keySystemsConfigs: IKeySystemOption[],
  contentProtections$ : Observable<IContentProtection>
) : Observable<IEMEManagerEvent> {
  log.debug("EME: Starting EMEManager logic.");

  /**
   * Contains information linked to initialization data processed for the
   * current content.
   * Allows to prevent unnecessary initialization data processing (e.g. avoid
   * unneeded license requests, unnecessary CDM negociations etc.).
   */
  const processedInitData : IProcessedDataItem[] = [];

  /**
   * When `true`, wait before processing newly-received initialization data.
   *
   * In certain cases where licenses might contain multiple keys, we might want
   * to avoid loading multiple licenses with keys in common. Using this lock to
   * prevent multiple parallel license requests allows to prevent that situation
   * from happening.
   * TODO this way of doing-it is very error-prone for now. A more readable
   * solution has to be found.
   */
  const initDataLock = createSharedReference<boolean>(false);

  /** Emit the MediaKeys instance and its related information when ready. */
  const mediaKeysInit$ = initMediaKeysAndSetServerCertificate(mediaElement,
                                                              keySystemsConfigs)
    .pipe(shareReplay()); // Share side-effects and cache success}

  /** Emit when the MediaKeys instance has been attached the HTMLMediaElement. */
  const attachedMediaKeys$ : Observable<IAttachedMediaKeysEvent> = mediaKeysInit$.pipe(
    filter((e) : e is IAttachedMediaKeysEvent => e.type === "attached-media-keys"),
    take(1));

  /** Parsed `encrypted` events coming from the HTMLMediaElement. */
  const mediaEncryptedEvents$ = onEncrypted$(mediaElement).pipe(
    tap((evt) => {
      log.debug("EME: Encrypted event received from media element.", evt);
    }),
    filterMap<MediaEncryptedEvent, IInitializationDataInfo, null>(
      (evt) => getInitData(evt), null));

  /** Encryption events coming from the `contentProtections$` argument. */
  const externalEvents$ = contentProtections$.pipe(
    tap((evt) => { log.debug("EME: Encrypted event received from Player", evt); }));

  /** Emit events signaling that an encryption initialization data is encountered. */
  const initializationData$ = observableMerge(externalEvents$, mediaEncryptedEvents$)
    .pipe(mergeMap((val => {
      return initDataLock.asObservable().pipe(
        filter(isLocked => !isLocked),
        take(1),
        mapTo(val));
    })));

  /** Create MediaKeySessions and handle the corresponding events. */
  const bindSession$ = initializationData$.pipe(

    // Add MediaKeys info once available
    mergeMap((initializationData) => attachedMediaKeys$.pipe(
      map((mediaKeysEvt) : [IInitializationDataInfo, IAttachedMediaKeysEvent] =>
        [ initializationData, mediaKeysEvt ]))),

    mergeMap(([initializationData, mediaKeysEvent]) => {
      /**
       * If set, previously-processed initialization data in the current
       * content is already compatible to this new initialization data.
       */
      const compatibleEntry = arrayFind(processedInitData, (x) => {
        return x.sessionInfo === null ?
          // XXX TODO
          x.record.isCompatibleWith(initializationData) :
          x.sessionInfo.record.isCompatibleWith(initializationData);
      });

      const { mediaKeySystemAccess, stores, options } = mediaKeysEvent.value;
      const { loadedSessionsStore } = mediaKeysEvent.value.stores;

      if (compatibleEntry !== undefined) {
        // We're already handling that initialization data in some way

        const { sessionInfo } = compatibleEntry;
        if (sessionInfo === null) {
          // A MediaKeySession is still in the process of being created for that
          // entry, ignore for now.
          // XXX TODO await before?
          return EMPTY;
        }

        // Check if the compatible initialization data is blacklisted
        const blacklistedSessionError = sessionInfo.blacklistedSessionError;
        if (!isNullOrUndefined(blacklistedSessionError)) {
          if (initializationData.type === undefined ||
              initializationData.content === undefined)
          {
            log.error("EME: This initialization data has already been blacklisted " +
                      "but the current content is not known.");
            return EMPTY;
          } else {
            log.info("EME: This initialization data has already been blacklisted. " +
                     "Blacklisting the related content.");
            const { manifest } = initializationData.content;
            manifest.addUndecipherableProtectionData(initializationData);
            return EMPTY;
          }
        }

        // Check if the current key id(s) is blacklisted
        if (sessionInfo.keyStatuses !== undefined &&
            initializationData.keyIds !== undefined)
        {
          /**
           * If set to `true`, the Representation(s) linked to this
           * initialization data's key id should be "blacklisted".
           */
          let shouldBlacklist;

          if (options.singleLicensePer === "init-data") {
            // Note: In the default "init-data" mode, we only blacklist a
            // Representation if the key id was originally explicitely
            // blacklisted (i.e. and not if its key id was just not present).
            //
            // This is to enforce v3.x.x retro-compatibility: we cannot
            // blacklist Representations unless some RxPlayer options
            // documentating this behavior have been set.
            const { blacklisted } = sessionInfo.keyStatuses;
            shouldBlacklist = areSomeKeyIdContainedIn(initializationData.keyIds,
                                                      blacklisted);
          } else {
            // In any other mode, we just blacklist as soon as not all of this
            // initialization data's linked key ids are explicitely whitelisted,
            // because we've no such retro-compatibility guarantee to make there.
            const { whitelisted } = sessionInfo.keyStatuses;
            shouldBlacklist = !areAllKeyIdContainedIn(initializationData.keyIds,
                                                      whitelisted);
          }

          if (shouldBlacklist) {
            if (initializationData.content === undefined) {
              log.error("EME: Cannot blacklist key id, the content is unknown.");
              return EMPTY;
            }
            log.info("EME: The encountered key id has been blacklisted.");
            initializationData.content.manifest.updateDeciperabilitiesBasedOnKeyIds({
              blacklistedKeyIDs: initializationData.keyIds,
              whitelistedKeyIds: [],
            });
            return EMPTY;
          }
        }

        // If we reached here, it means that this initialization data is not
        // blacklisted in any way.
        // Search loaded session and put it on top of the cache if it exists.
        const entry = loadedSessionsStore.reuse(initializationData);
        if (entry !== null) {
          log.debug("EME: Init data already processed. Skipping it.");
          return EMPTY;
        }

        // Session not found in `loadedSessionsStore`, it might have been closed
        // since.
        // Remove from `processedInitData` and start again.
        const indexOf = processedInitData.indexOf(compatibleEntry);
        if (indexOf === -1) {
          log.error("EME: Unable to remove processed init data: not found.");
        } else {
          log.debug("EME: A session from a processed init data is not available " +
                    "anymore. Re-processing it.");
          processedInitData.splice(indexOf, 1);
        }
      }

      // If we reached here, we did not handle this initialization data yet for
      // the current content.

      if (options.singleLicensePer === "content") {
        const firstCreatedSession = arrayFind(processedInitData, (x) =>
          x.sessionInfo?.source === "new-session");

        if (firstCreatedSession !== undefined) {
          // We already fetched a `singleLicensePer: "content"` license, yet the
          // current initialization data was not yet handled.
          // It means that we'll never handle it and we should thus blacklist it.

          const keyIds = initializationData.keyIds;
          if (keyIds === undefined) {
            log.warn("EME: Initialization data linked to unknown key id, we'll " +
              "not able to fallback from it.");
            return EMPTY;
          }

          firstCreatedSession.record.associateKeyIds(keyIds);
          if (initializationData.content !== undefined) {
            initializationData.content.manifest
              .updateDeciperabilitiesBasedOnKeyIds({ blacklistedKeyIDs: keyIds,
                                                     whitelistedKeyIds: [] });
          }
          return EMPTY;
        }

        // Because we typically only want to create a single new session in a
        // `singleLicensePer: "content"` mode, we will temprarily lock new
        // initialization data from being processed while we're still
        // processing that one.
        initDataLock.setValue(true);
      }

      /** `IProcessedDataItem` linked to that initialization data. */
      const currentInitItem : IProcessedDataItem = {
        record: new ProcessedInitDataRecord(initializationData),
        sessionInfo: null,
      };
      processedInitData.push(currentInitItem);

      let wantedSessionType : MediaKeySessionType;
      if (options.persistentLicense !== true) {
        wantedSessionType = "temporary";
      } else if (!canCreatePersistentSession(mediaKeySystemAccess)) {
        log.warn("EME: Cannot create \"persistent-license\" session: not supported");
        wantedSessionType = "temporary";
      } else {
        wantedSessionType = "persistent-license";
      }

      const maxSessionCacheSize = typeof options.maxSessionCacheSize === "number" ?
        options.maxSessionCacheSize :
        EME_DEFAULT_MAX_SIMULTANEOUS_MEDIA_KEY_SESSIONS;
      return createOrLoadSession(initializationData,
                                 currentInitItem.record,
                                 stores,
                                 wantedSessionType,
                                 maxSessionCacheSize)
        .pipe(mergeMap((sessionEvt) =>  {
          let generateRequest$ = EMPTY;
          let sessionInfo : IProcessedDataItemSessionInfo;

          switch (sessionEvt.type) {
            case "created-session":
              sessionInfo = {
                record: sessionEvt.value.initDataRecord,
                source: "new-session" as const,
                keyStatuses: undefined,
                blacklistedSessionError: null,
              };

              // `generateKeyRequest` awaits a single Uint8Array containing all
              // initialization data.
              const concatInitData =
                concat(...initializationData.values.map(i => i.data));
              generateRequest$ = generateKeyRequest(sessionEvt.value.mediaKeySession,
                                                    initializationData.type,
                                                    concatInitData).pipe(
                catchError((error: unknown) => {
                  throw new EncryptedMediaError(
                    "KEY_GENERATE_REQUEST_ERROR",
                    error instanceof Error ? error.toString() :
                    "Unknown error");
                }),
                ignoreElements());
              break;
            case "loaded-open-session":
              sessionInfo = { record: sessionEvt.value.initDataRecord,
                              source: "from-cache" as const,
                              keyStatuses: undefined,
                              blacklistedSessionError: null };
              break;
            case "loaded-persistent-session":
              sessionInfo = { record: sessionEvt.value.initDataRecord,
                              source: "from-persisted" as const,
                              keyStatuses: undefined,
                              blacklistedSessionError: null };
              break;

            default: // Use TypeScript to check if all possibilities have been checked
              assertUnreachable(sessionEvt);
          }

          currentInitItem.sessionInfo = sessionInfo;

          const { mediaKeySession,
                  sessionType } = sessionEvt.value;

          /**
           * We only store persistent sessions once its keys are known.
           * This boolean allows to know if this session has already been
           * persisted or not.
           */
          let isSessionPersisted = false;

          return observableMerge(SessionEventsListener(mediaKeySession,
                                                       options,
                                                       mediaKeySystemAccess.keySystem,
                                                       initializationData),
                                 generateRequest$)
            .pipe(
              mergeMap(function onSessionEvent(evt) {
                if (evt.type !== "keys-update") {
                  return observableOf(evt);
                }

                // We want to add the current key ids in the blacklist if it is
                // not already there.
                //
                // We only do that when `singleLicensePer` is set to something
                // else than the default `"init-data"` because this logic:
                //   1. might result in a quality fallback, which is a v3.x.x
                //      breaking change if some APIs (like `singleLicensePer`)
                //      aren't used.
                //   2. Rely on the EME spec regarding key statuses being well
                //      implemented on all supported devices, which we're not
                //      sure yet. Because in any other `singleLicensePer`, we
                //      need a good implementation anyway, it doesn't matter
                //      there.
                const expectedKeyIds = initializationData.keyIds;
                if (expectedKeyIds !== undefined &&
                    options.singleLicensePer !== "init-data")
                {
                  const missingKeyIds = expectedKeyIds.filter(expected => {
                    return (
                      !evt.value.whitelistedKeyIds.some(whitelisted =>
                        areArraysOfNumbersEqual(whitelisted, expected)) &&
                      !evt.value.blacklistedKeyIDs.some(blacklisted =>
                        areArraysOfNumbersEqual(blacklisted, expected))
                    );
                  });
                  if (missingKeyIds.length > 0) {
                    evt.value.blacklistedKeyIDs.push(...missingKeyIds) ;
                  }
                }

                const allKeyStatuses = [...evt.value.whitelistedKeyIds,
                                        ...evt.value.blacklistedKeyIDs];
                sessionInfo.record.associateKeyIds(allKeyStatuses);
                sessionInfo.keyStatuses = {
                  whitelisted: evt.value.whitelistedKeyIds,
                  blacklisted: evt.value.blacklistedKeyIDs,
                };

                if ((evt.value.whitelistedKeyIds.length !== 0 ||
                     evt.value.blacklistedKeyIDs.length !== 0) &&
                    sessionType === "persistent-license" &&
                    stores.persistentSessionsStore !== null &&
                    isSessionPersisted)
                {
                  const { persistentSessionsStore } = stores;
                  cleanOldStoredPersistentInfo(
                    persistentSessionsStore,
                    EME_MAX_STORED_PERSISTENT_SESSION_INFORMATION - 1);
                  persistentSessionsStore.add(currentInitItem.record,
                                              mediaKeySession);
                  isSessionPersisted = true;
                }
                if (initializationData.content !== undefined) {
                  initializationData.content.manifest
                    .updateDeciperabilitiesBasedOnKeyIds(evt.value);
                }

                // Now that key ids update have been processed, we can remove
                // the lock if it was active.
                initDataLock.setValue(false);
                return EMPTY;
              }),

              catchError(function onSessionError(err) {
                if (!(err instanceof BlacklistedSessionError)) {
                  initDataLock.setValue(false);
                  throw err;
                }

                sessionInfo.blacklistedSessionError = err;

                const { sessionError } = err;
                if (initializationData.type === undefined) {
                  log.error("EME: Current session blacklisted and content not known. " +
                            "Throwing.");
                  sessionError.fatal = true;
                  throw sessionError;
                }

                log.warn("EME: Current session blacklisted. Blacklisting content.");
                if (initializationData.content !== undefined) {
                  const { manifest } = initializationData.content;
                  log.info("Init: blacklisting Representations based on " +
                           "protection data.");
                  manifest.addUndecipherableProtectionData(initializationData);
                }

                initDataLock.setValue(false);
                return observableOf({ type: "warning" as const,
                                      value: sessionError });
              }));
        }));
    }));

  return observableMerge(mediaKeysInit$, bindSession$);
}

/**
 * Returns `true` if the given MediaKeySystemAccess can create
 * "persistent-license" MediaKeySessions.
 * @param {MediaKeySystemAccess} mediaKeySystemAccess
 * @returns {Boolean}
 */
function canCreatePersistentSession(
  mediaKeySystemAccess : MediaKeySystemAccess | ICustomMediaKeySystemAccess
) : boolean {
  const { sessionTypes } = mediaKeySystemAccess.getConfiguration();
  return sessionTypes !== undefined &&
         arrayIncludes(sessionTypes, "persistent-license");
}

/**
 * @param {HTMLMediaElement} mediaElement - The MediaElement which will be
 * associated to a MediaKeys object
 * @param {Array.<Object>} keySystemsConfigs - key system configuration
 * @returns {Observable}
 */
function initMediaKeysAndSetServerCertificate(
  mediaElement : HTMLMediaElement,
  keySystemsConfigs: IKeySystemOption[]
) : Observable<IEMEWarningEvent | IAttachedMediaKeysEvent | ICreatedMediaKeysEvent> {
  return initMediaKeys(mediaElement, keySystemsConfigs).pipe(mergeMap((mediaKeysEvt) => {
    if (mediaKeysEvt.type !== "attached-media-keys") {
      return observableOf(mediaKeysEvt);
    }
    const { mediaKeys, options } = mediaKeysEvt.value;
    const { serverCertificate } = options;
    if (isNullOrUndefined(serverCertificate)) {
      return observableOf(mediaKeysEvt);
    }
    return observableConcat(setServerCertificate(mediaKeys, serverCertificate),
                            observableOf(mediaKeysEvt));
  }));
}

/**
 * Data relative to encryption initialization data already handled for the
 * current content.
 */
interface IProcessedDataItem {
  /**
   * Linked ProcessedInitDataRecord.
   * Allows to check for compatibility with future incoming initialization
   * data.
   */
  // XXX TODO
  record : ProcessedInitDataRecord;

  sessionInfo : null | IProcessedDataItemSessionInfo;
}

interface IProcessedDataItemSessionInfo {
  record : ProcessedInitDataRecord;

  keyStatuses : undefined | {
    whitelisted : Uint8Array[];
    blacklisted : Uint8Array[];
  };

  /**
   * Source of the MediaKeySession linked to that record:
   *   - `undefined`: No MediaKeySession linked to that initialization data
   *     has been created or its `sessionSource` is not known yet.
   *   - `"new-session"`: A new MediaKeySession, necessitating a license
   *     request, has been created in the process of treating this
   *     initialization data.
   *   - `"from-cache"`: A MediaKeySession retreived from a cache has been
   *     reused to handle that initialization data, thus preventing the need
   *     to perform a new license request.
   *   - `"from-persisted"`: A persisted MediaKeySession was loaded to handle
   *     that initialization data.
   */
  source : "from-cache" | "new-session" | "from-persisted";

  /**
   * If different than `null`, all initialization data compatible with this
   * processed initialization data has been blacklisted with this corresponding
   * error.
   */
  blacklistedSessionError : BlacklistedSessionError | null;
}
