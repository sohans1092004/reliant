// Pure event validation, used by the ingest API (reject bad input) and the
// consumer (detect poison-pill messages that can never be processed).

const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
const NAME = /^[a-z0-9_.-]{1,100}$/i;

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
