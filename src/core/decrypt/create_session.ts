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

import PPromise from "pinkie";
import {
  ICustomMediaKeySession,
  loadSession,
} from "../../compat";
import log from "../../log";
import {
  IInitializationDataInfo,
  IMediaKeySessionStores,
  MediaKeySessionLoadingType,
} from "./types";
import isSessionUsable from "./utils/is_session_usable";
import LoadedSessionsStore from "./utils/loaded_sessions_store";
import PersistentSessionsStore from "./utils/persistent_sessions_store";
import KeySessionRecord from "./utils/processed_init_data_record";

/**
 * Create a new Session or load a persistent one on the given MediaKeys,
 * according to wanted settings and what is currently stored.
 *
 * If session creating fails, remove the oldest MediaKeySession loaded and
 * retry.
 *
 * /!\ This only creates new sessions.
 * It will fail if loadedSessionsStore already has a MediaKeySession with
 * the given initialization data.
 * @param {Object} stores
 * @param {Object} initData
 * @param {string} wantedSessionType
 * @returns {Promise}
 */
export default function createSession(
  stores : IMediaKeySessionStores,
  initData : IInitializationDataInfo,
  wantedSessionType : MediaKeySessionType
) : Promise<ICreateSessionEvent> {
  const { loadedSessionsStore,
          persistentSessionsStore } = stores;

  if (wantedSessionType === "temporary") {
    return createTemporarySession(loadedSessionsStore, initData);
  } else if (persistentSessionsStore === null) {
    log.warn("DRM: Cannot create persistent MediaKeySession, " +
             "PersistentSessionsStore not created.");
    return createTemporarySession(loadedSessionsStore, initData);
  }
  return createAndTryToRetrievePersistentSession(loadedSessionsStore,
                                                 persistentSessionsStore,
                                                 initData);
}

/**
 * Create a new temporary MediaKeySession linked to the given initData and
 * initDataType.
 * @param {Object} loadedSessionsStore
 * @param {Object} initData
 * @returns {Promise}
 */
function createTemporarySession(
  loadedSessionsStore : LoadedSessionsStore,
  initData : IInitializationDataInfo
) : Promise<INewSessionCreatedEvent> {
  log.info("DRM: Creating a new temporary session");
  const entry = loadedSessionsStore.createSession(initData, "temporary");
  return PPromise.resolve({ type: MediaKeySessionLoadingType.Created,
                            value: entry });
}

/**
 * Create a persistent MediaKeySession and try to load on it a previous
 * MediaKeySession linked to the same initialization data.
 * @param {Object} loadedSessionsStore
 * @param {Object} persistentSessionsStore
 * @param {Object} initData
 * @returns {Promise}
 */
async function createAndTryToRetrievePersistentSession(
  loadedSessionsStore : LoadedSessionsStore,
  persistentSessionsStore : PersistentSessionsStore,
  initData : IInitializationDataInfo
) : Promise<INewSessionCreatedEvent | IPersistentSessionRecoveryEvent> {
  log.info("DRM: Creating persistent MediaKeySession");

  const entry = loadedSessionsStore.createSession(initData, "persistent-license");
  const storedEntry = persistentSessionsStore.getAndReuse(entry.keySessionRecord);
  if (storedEntry === null) {
    return { type: MediaKeySessionLoadingType.Created,
             value: entry };
  }

  try {
    const hasLoadedSession = await loadSession(entry.mediaKeySession,
                                               storedEntry.sessionId);
    if (!hasLoadedSession) {
      log.warn("DRM: No data stored for the loaded session");
      persistentSessionsStore.delete(entry.keySessionRecord);
      return { type: MediaKeySessionLoadingType.Created,
               value: entry };
    }

    if (hasLoadedSession && isSessionUsable(entry.mediaKeySession)) {
      persistentSessionsStore.add(entry.keySessionRecord, entry.mediaKeySession);
      log.info("DRM: Succeeded to load persistent session.");
      return { type: MediaKeySessionLoadingType.LoadedPersistentSession,
               value: entry };
    }

    // Unusable persistent session: recreate a new session from scratch.
    log.warn("DRM: Previous persistent session not usable anymore.");
    return recreatePersistentSession();
  } catch (err) {
    log.warn("DRM: Unable to load persistent session: " +
             (err instanceof Error ? err.toString() :
                                     "Unknown Error"));
    return recreatePersistentSession();
  }

  /**
   * Helper function to close and restart the current persistent session
   * considered, and re-create it from scratch.
   * @returns {Observable}
   */
  async function recreatePersistentSession() : Promise<INewSessionCreatedEvent> {
    log.info("DRM: Removing previous persistent session.");
    if (persistentSessionsStore.get(entry.keySessionRecord) !== null) {
      persistentSessionsStore.delete(entry.keySessionRecord);
    }

    await loadedSessionsStore.closeSession(entry.mediaKeySession);
    const newEntry = loadedSessionsStore.createSession(initData,
                                                       "persistent-license");
    return { type: MediaKeySessionLoadingType.Created,
             value: newEntry };
  }
}

export interface INewSessionCreatedEvent {
  type : MediaKeySessionLoadingType.Created;
  value : {
    mediaKeySession : MediaKeySession |
                      ICustomMediaKeySession;
    sessionType : MediaKeySessionType;
    keySessionRecord : KeySessionRecord;
  };
}

export interface IPersistentSessionRecoveryEvent {
  type : MediaKeySessionLoadingType.LoadedPersistentSession;
  value : {
    mediaKeySession : MediaKeySession |
                      ICustomMediaKeySession;
    sessionType : MediaKeySessionType;
    keySessionRecord : KeySessionRecord;
  };
}

export type ICreateSessionEvent = INewSessionCreatedEvent |
                                  IPersistentSessionRecoveryEvent;