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
var __spreadArray = (this && this.__spreadArray) || function (to, from, pack) {
    if (pack || arguments.length === 2) for (var i = 0, l = from.length, ar; i < l; i++) {
        if (ar || !(i in from)) {
            if (!ar) ar = Array.prototype.slice.call(from, 0, i);
            ar[i] = from[i];
        }
    }
    return to.concat(ar || Array.prototype.slice.call(from));
};
import objectAssign from "../../../../utils/object_assign";
// eslint-disable-next-line max-len
import extractMinimumAvailabilityTimeOffset from "./extract_minimum_availability_time_offset";
import { BaseRepresentationIndex, ListRepresentationIndex, TemplateRepresentationIndex, TimelineRepresentationIndex, } from "./indexes";
import resolveBaseURLs from "./resolve_base_urls";
/**
 * Parse the specific segment indexing information found in a representation
 * into a IRepresentationIndex implementation.
 * @param {Array.<Object>} representation
 * @param {Object} representationInfos
 * @returns {Array.<Object>}
 */
export default function parseRepresentationIndex(representation, representationInfos) {
    var _a, _b;
    var representationBaseURLs = resolveBaseURLs(representationInfos.baseURLs, representation.children.baseURLs);
    var aggressiveMode = representationInfos.aggressiveMode, availabilityTimeOffset = representationInfos.availabilityTimeOffset, manifestBoundsCalculator = representationInfos.manifestBoundsCalculator, isDynamic = representationInfos.isDynamic, periodEnd = representationInfos.end, periodStart = representationInfos.start, receivedTime = representationInfos.receivedTime, timeShiftBufferDepth = representationInfos.timeShiftBufferDepth, unsafelyBaseOnPreviousRepresentation = representationInfos.unsafelyBaseOnPreviousRepresentation, inbandEventStreams = representationInfos.inbandEventStreams;
    var isEMSGWhitelisted = function (inbandEvent) {
        if (inbandEventStreams === undefined) {
            return false;
        }
        return inbandEventStreams
            .some(function (_a) {
            var schemeIdUri = _a.schemeIdUri;
            return schemeIdUri === inbandEvent.schemeIdUri;
        });
    };
    var context = { aggressiveMode: aggressiveMode, availabilityTimeOffset: availabilityTimeOffset, unsafelyBaseOnPreviousRepresentation: unsafelyBaseOnPreviousRepresentation, isEMSGWhitelisted: isEMSGWhitelisted, manifestBoundsCalculator: manifestBoundsCalculator, isDynamic: isDynamic, periodEnd: periodEnd, periodStart: periodStart, receivedTime: receivedTime, representationBaseURLs: representationBaseURLs, representationBitrate: representation.attributes.bitrate,
        representationId: representation.attributes.id, timeShiftBufferDepth: timeShiftBufferDepth };
    var representationIndex;
    if (representation.children.segmentBase !== undefined) {
        var segmentBase = representation.children.segmentBase;
        context.availabilityTimeOffset =
            representationInfos.availabilityTimeOffset +
                extractMinimumAvailabilityTimeOffset(representation.children.baseURLs) +
                ((_a = segmentBase.availabilityTimeOffset) !== null && _a !== void 0 ? _a : 0);
        representationIndex = new BaseRepresentationIndex(segmentBase, context);
    }
    else if (representation.children.segmentList !== undefined) {
        var segmentList = representation.children.segmentList;
        representationIndex = new ListRepresentationIndex(segmentList, context);
    }
    else if (representation.children.segmentTemplate !== undefined ||
        representationInfos.parentSegmentTemplates.length > 0) {
        var segmentTemplates = representationInfos.parentSegmentTemplates.slice();
        var childSegmentTemplate = representation.children.segmentTemplate;
        if (childSegmentTemplate !== undefined) {
            segmentTemplates.push(childSegmentTemplate);
        }
        var segmentTemplate = objectAssign.apply(void 0, __spreadArray([{}], segmentTemplates /* Ugly TS Hack */, false));
        context.availabilityTimeOffset =
            representationInfos.availabilityTimeOffset +
                extractMinimumAvailabilityTimeOffset(representation.children.baseURLs) +
                ((_b = segmentTemplate.availabilityTimeOffset) !== null && _b !== void 0 ? _b : 0);
        representationIndex = TimelineRepresentationIndex
            .isTimelineIndexArgument(segmentTemplate) ?
            new TimelineRepresentationIndex(segmentTemplate, context) :
            new TemplateRepresentationIndex(segmentTemplate, context);
    }
    else {
        var adaptationChildren = representationInfos.adaptation.children;
        if (adaptationChildren.segmentBase !== undefined) {
            var segmentBase = adaptationChildren.segmentBase;
            representationIndex = new BaseRepresentationIndex(segmentBase, context);
        }
        else if (adaptationChildren.segmentList !== undefined) {
            var segmentList = adaptationChildren.segmentList;
            representationIndex = new ListRepresentationIndex(segmentList, context);
        }
        else {
            representationIndex = new TemplateRepresentationIndex({
                duration: Number.MAX_VALUE,
                timescale: 1,
                startNumber: 0,
                media: "",
            }, context);
        }
    }
    return representationIndex;
}
