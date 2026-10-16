// Pure event validation, used by the ingest API (reject bad input) and the
// consumer (detect poison-pill messages that can never be processed).

const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
const NAME = /^[a-z0-9_.-]{1,100}$/i;

// An endpoint's subscription: null (every event type) or a list of event types.
// Returns a problem string, or null if valid.
export function validateEventTypes(v) {
  if (v === null) return null;
  if (!Array.isArray(v) || v.length < 1 || v.length > 50) return 'event_types must be null (all types) or a list of 1-50 types';
  if (!v.every((t) => typeof t === 'string' && NAME.test(t))) return 'each event type must be 1-100 chars of [a-z0-9_.-]';
  return null;
}

// Returns a list of problems; empty list = valid.
export function validateEvent(e) {
  if (!e || typeof e !== 'object' || Array.isArray(e)) return ['event must be a JSON object'];
  const errors = [];
  if (e.event_id !== undefined && !UUID.test(e.event_id)) errors.push('event_id must be a UUID');
  if (typeof e.tenant_id !== 'string' || !NAME.test(e.tenant_id)) errors.push('tenant_id must be 1-100 chars of [a-z0-9_.-]');
  if (typeof e.type !== 'string' || !NAME.test(e.type)) errors.push('type must be 1-100 chars of [a-z0-9_.-]');
  if (!e.payload || typeof e.payload !== 'object' || Array.isArray(e.payload)) errors.push('payload must be a JSON object');
  return errors;
}
