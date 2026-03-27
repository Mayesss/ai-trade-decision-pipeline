import {
  listScalpEntrySessionProfiles,
  parseScalpEntrySessionProfileStrict,
} from "../scalp/sessions";

export function listScalpV2EntrySessionProfiles() {
  return listScalpEntrySessionProfiles();
}

export function parseScalpV2EntrySessionProfileStrict(value: unknown) {
  return parseScalpEntrySessionProfileStrict(value);
}
